#!/usr/bin/env bash

# A credential embedded in a broker address, in an XDBC connection string or in a URL-valued setting
# must not reach `system.query_log`. Every statement below is rejected, and none of them needs a
# broker, a bridge or an existing named collection: masking runs when the statement is formatted for
# logging, before it is validated. Each positive case is paired with the control that has to stay
# fully visible, and each credential is a distinct `leak05233*` canary, so a leak points straight at
# the site that leaked it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="05233_${CLICKHOUSE_DATABASE}_$(random_str 8)"
ARMS=0

arm() { # arm <name> <statement>
    ARMS=$((ARMS + 1))
    $CLICKHOUSE_CLIENT --query_id="${PREFIX}_$1" --log_queries=1 -q "$2" > /dev/null 2>&1
}

# `nats_url`: libnats ends the userinfo at the last '@' of the whole value, with the scheme optional
# and the value trimmed, so a password containing '/' and a scheme-less address both authenticate.
arm a1_nats_slash_password   "SET nats_url = 'nats://u:pa/leak05233nats@h:4222'"
arm a2_nats_no_scheme        "SET nats_url = 'u:leak05233noscheme@h:4222'"
arm a3_nats_leading_space    "SET nats_url = ' nats://u:pa/leak05233space@h:4222'"
arm a4_nats_control          "SET nats_url = 'localhost:4222'"

# `rabbitmq_address`: AMQP-CPP ends the login at the first '@' after the scheme, also unbounded by the
# '/' that closes an RFC 3986 authority.
arm b1_amqp_slash_password   "SET rabbitmq_address = 'amqp://u:pa/leak05233amqp@h:5672/v'"
arm b2_amqp_control          "SET rabbitmq_address = 'amqp://h:5672/v'"

# The engine-argument form has its own masking site and must agree with the `SETTINGS` clause above.
arm c1_nats_engine_argument  "CREATE TABLE t05233 (x UInt8) ENGINE = NATS(nc05233, nats_url = 'nats://u:pa/leak05233arg@h:4222')"
arm c2_nats_engine_control   "CREATE TABLE t05233 (x UInt8) ENGINE = NATS(nc05233, nats_url = 'localhost:4222')"

# `Kafka`, `NATS` and `RabbitMQ` all read their settings as named overrides of the collection, so an
# override of a secret setting needs the same masking as the `SETTINGS` clause. Kafka's legacy
# positional form carries no secret, so its positional arguments stay visible.
arm f1_rabbitmq_engine_argument  "CREATE TABLE t05233 (x UInt8) ENGINE = RabbitMQ(nc05233, rabbitmq_address = 'amqp://u:leak05233rmqarg@h:5672/v')"
arm f2_rabbitmq_engine_password  "CREATE TABLE t05233 (x UInt8) ENGINE = RabbitMQ(nc05233, rabbitmq_password = 'leak05233rmqpw')"
arm f3_rabbitmq_engine_control   "CREATE TABLE t05233 (x UInt8) ENGINE = RabbitMQ(nc05233, rabbitmq_address = 'amqp://h:5672/v')"
arm f4_kafka_engine_password     "CREATE TABLE t05233 (x UInt8) ENGINE = Kafka(nc05233, kafka_sasl_password = 'leak05233kafkapw')"
arm f5_kafka_positional_control  "CREATE TABLE db05233absent.t05233 (x UInt8) ENGINE = Kafka('broker05233:9092', 'topic05233', 'group05233', 'JSONEachRow')"
# That positional form also makes the collection name optional, so a named override can be the first
# argument. The positional beside it stays visible, so this arm is its own over-masking control.
arm f6_kafka_first_argument_secret "CREATE TABLE t05233 (x UInt8) ENGINE = Kafka(kafka_sasl_password = 'leak05233kafkafirst', 'clickhouse')"

# An XDBC connection string is forwarded to the driver verbatim, so its grammar is the driver's: the
# password can sit in a query parameter or in a `KEY=value;` list, and no URI scan bounds it. The
# positional form is written against a database this test never creates, because a `jdbc(...)` table
# function that reaches argument validation then spends 30 seconds trying to start the bridge.
arm d1_jdbc_query_parameter  "CREATE TABLE db05233absent.t05233 (x UInt8) ENGINE = JDBC('mysql://u:p@h/?password=leak05233param', 'db', 't')"
arm d2_jdbc_at_in_password   "CREATE TABLE db05233absent.t05233 (x UInt8) ENGINE = JDBC('jdbc://user:pa@leak05233at@h:5432/db', 'db', 't')"
arm d3_odbc_key_value_form   "CREATE TABLE db05233absent.t05233 (x UInt8) ENGINE = ODBC('DSN=x;Uid=u;Pwd=leak05233kv', 'db', 't')"
arm d4_odbc_named_argument   "SELECT * FROM odbc(nc05233, connection_settings = 'odbc://u:pa@leak05233odbc@h/db')"
arm d5_jdbc_duplicate_key    "SELECT * FROM jdbc(nc05233, datasource = 'a://u:leak05233dup1@h/1', datasource = 'b://u:leak05233dup2@h/2')"
arm d6_jdbc_computed_key     "SELECT * FROM jdbc(nc05233, concat('data', 'source') = 'jdbc://u:leak05233computed@h/db')"
arm d7_jdbc_both_aliases     "SELECT * FROM jdbc(nc05233, datasource = 'a://u:leak05233both1@h/1', connection_settings = 'b://u:leak05233both2@h/2')"
# Control for the fail-closed unreadable-key scan the XDBC branch now shares with the TLS keys.
arm d8_mysql_computed_key    "SELECT * FROM mysql(nc05233, concat('ssl_ca', '_pem') = 'leak05233mysql', table = 't')"
# After the collection name every argument must be named, so a positional one is hidden whole.
arm d9_jdbc_positional_after_collection "SELECT * FROM jdbc(nc05233, 'jdbc://u:leak05233pos@h/db')"
# A named argument at index 0 is not a collection name, so the connection string can then sit at any
# index under either alias.
arm d10_jdbc_named_without_collection "SELECT * FROM jdbc(external_table = 't05233', datasource = 'DSN=x;Uid=u;Pwd=leak05233nocoll')"

# The query-level URL settings are read through `Poco::URI`, so host and path stay visible and only
# the userinfo is hidden. A value with no scheme in front of it is hidden whole instead.
arm e1_avro_at_in_password   "SELECT 1 SETTINGS format_avro_schema_registry_url = 'http://user:pa@leak05233avro@reg:8080/'"
arm e2_avro_userinfo_only    "SELECT 1 SETTINGS format_avro_schema_registry_url = 'http://leak05233token@reg:8080/'"
arm e3_url_base_no_userinfo  "SELECT 1 SETTINGS url_base = 'http://h:8080/d/?email=a@b.com'"
arm e4_url_base_no_scheme    "SELECT 1 SETTINGS url_base = ' http://u:leak05233space@h/d/'"
arm e5_url_base_control      "SELECT 1 SETTINGS url_base = 'https://h/d/'"
arm e6_s3_base_presigned     "SELECT 1 SETTINGS s3_base = 'https://b/f.csv?X-Amz-Signature=leak05233sig'"
# All three URL settings follow the one rule, for every scheme: the userinfo is hidden and the rest
# stays visible, and a value whose scheme is not at the start is hidden whole.
arm e7_url_base_abfss        "SELECT 1 SETTINGS url_base = 'abfss://container@account.dfs.core.windows.net/d/'"
arm e8_avro_abfss_control    "SELECT 1 SETTINGS format_avro_schema_registry_url = 'abfss://u:leak05233avroabfss@h/'"
arm e9_url_base_az_control   "SELECT 1 SETTINGS url_base = 'az://u:leak05233az@account.blob.core.windows.net/c/'"
arm e10_url_base_abfss_space "SELECT 1 SETTINGS url_base = ' abfss://c@a.dfs.core.windows.net/d/'"
arm e12_url_base_abfss_password "SELECT 1 SETTINGS url_base = 'abfss://c:leak05233abfs@a.dfs.core.windows.net/d/'"
# The same setting reached through an `ENGINE = ... SETTINGS` clause rather than through a query.
arm e11_avro_in_engine       "CREATE TABLE t05233 (x UInt8) ENGINE = NATS(nc05233) SETTINGS format_avro_schema_registry_url = 'http://user:pa@leak05233engine@reg:8080/'"

for _ in {1..120}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    LOGGED=$($CLICKHOUSE_CLIENT -q "SELECT uniqExact(query_id) FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id LIKE '${PREFIX}\_%'")
    [ "$LOGGED" -ge "$ARMS" ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT replaceOne(query_id, '${PREFIX}_', '') AS arm, max(query)
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id LIKE '${PREFIX}\_%'
    GROUP BY arm ORDER BY arm FORMAT TSVRaw"

# No canary in any column that prints the statement or its settings, in any arm.
$CLICKHOUSE_CLIENT -q "SELECT countIf(position(concat(query, formatted_query, toString(Settings)), 'leak05233') > 0)
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id LIKE '${PREFIX}\_%'"
