#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Kafka engine is not built in the fast test.

# Credentials can sit in the settings of the engine rather than in its arguments: `kafka_sasl_password`
# is masked in `SHOW CREATE TABLE` all the same, so inheriting it needs SELECT on the source table too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

# The broker is never contacted, only the definition is copied.
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

# Overriding the masked setting means it is not inherited, so the copy needs nothing more.
echo "with the password overridden:"
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.own_password AS ${db}.kafka_src SETTINGS kafka_sasl_password = 'own'"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'own_password'"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.kafka_src TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.copy_of_kafka_src AS ${db}.kafka_src"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'copy_of_kafka_src'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
