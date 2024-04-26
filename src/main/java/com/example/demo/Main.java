package com.example.demo;

import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        runMqtt();
    }

    public static void runMqtt() {
        for (int i = 0; i < 100; i++) {
            extracted(i + 1);

        }
    }

    private static void extracted(int i) {
        String src = """
                function main(data) { var datac = JSON.parse(data).Timestamp; return datac; }
                """;
        String src2 = """
                function main(data) {
                    var datac = JSON.parse(data).Data;
                    var parse = JSON.parse(datac);
                    var keyValuePairs = {};
                    return [parse]
                }
                """;

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");

        String broker = "tcp://localhost:1883";
        String clientId = "java-mqtt-" + i;

        MqttClient client;
        try {
            client = new MqttClient(broker, clientId, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName("admin");
            options.setPassword("admin123".toCharArray());
            client.connect(options);

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Disconnected from MQTT broker");
                    try {
                        client.subscribe("topic/" + i, 0); // QoS: 0 = At Most Once
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message)
                        throws Exception {
                    String jsonData = new String(message.getPayload(), StandardCharsets.UTF_8);
                    long time = getTimestamp(engine, src, jsonData);
//                    System.out.println(time);
                    handlerData(engine, src2, jsonData);
                    timeSub(time, 1);
                }

                @Override
                public void deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken token) {
                    // Not used in this case
                }
            });

            System.out.println("Connected to MQTT broker");

            client.subscribe("topic/" + i, 0); // QoS: 0 = At Most Once

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static long getTimestamp(ScriptEngine engine, String src, String data)
            throws ScriptException, NoSuchMethodException {
        engine.eval(src);
        Invocable invocable = (Invocable) engine;
        double res = (double) invocable.invokeFunction("main", data);
        return (long) res;
    }

    public static void handlerData(ScriptEngine engine, String src, String data)
            throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        engine.eval(src);
        Invocable invocable = (Invocable) engine;
        Object res = invocable.invokeFunction("main", data);
        String s = JSONUtil.toJsonStr(res);

        List<Map<String, Object>> list = objectMapper.readValue(s,
                new TypeReference<List<Map<String, Object>>>() {
                });
        for (Map<String, Object> objectMap : list) {
            for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
//                System.out.println(entry.getKey() + "  " + entry.getValue());
            }
        }
    }

    public static long getNow() {
        return Instant.now().toEpochMilli();
    }

    public static void timeSub(long upTime, int topic) {
        long now = getNow();
        long diff = now - upTime;
        System.out.println(" 时间差 = " + diff + " 毫秒");
    }
}
