/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package picoreti;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableTestJob {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/avro.html
        tEnv.executeSql("CREATE TABLE orders (\n" +
                " id BIGINT,\n" +
                " total FLOAT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'postgres.public.torder',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'avro',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")");

        Table result = tEnv.sqlQuery(
                "SELECT id, total FROM orders WHERE id > 0");


        tEnv.toAppendStream(result, Order.class).print();

        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        env.execute();
    }

    public static class Order {
        public int id;
        public String order;
        public double total;

        public Order(int id, String order, double total) {
            this.id = id;
            this.order = order;
            this.total = total;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "id=" + id +
                    ", order='" + order + '\'' +
                    ", total=" + total +
                    '}';
        }
    }
}
