#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test does not build the Kafka engine

# the engine settings can hold credentials, not only the engine arguments. SHOW CREATE TABLE masks
# kafka_sasl_password too, so a copy of it also needs SELECT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

# the server does not contact the broker. it only copies the definition
${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT KAFKA ON *.* TO ${user};

    CREATE TABLE ${db}.kafka_src (id UInt64) ENGINE = Kafka
        SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'topic',
                 kafka_group_name = 'group', kafka_format = 'CSV', kafka_sasl_password = 'secret';

    GRANT SHOW COLUMNS ON ${db}.kafka_src TO ${user};
"

echo "with SHOW COLUMNS only:"
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.copy_of_kafka_src AS ${db}.kafka_src" 2>&1 \
    | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"

# the copy does not inherit a setting that this query gives, so it needs nothing more
echo "with a password in this query:"
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.own_password AS ${db}.kafka_src SETTINGS kafka_sasl_password = 'own'"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'own_password'"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.kafka_src TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.copy_of_kafka_src AS ${db}.kafka_src"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'copy_of_kafka_src'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
