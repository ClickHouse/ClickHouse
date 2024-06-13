#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: dictionaries will be updated according to server TZ, not session, so prohibit it's randomization
$CLICKHOUSE_CLIENT --session_timezone '' -q "
    CREATE TABLE table_for_update_field_dictionary
    (
        key UInt64,
        value String,
        last_insert_time DateTime
    )
    ENGINE = TinyLog
"

layouts=(
    flat
    hashed
    complex_key_hashed
)
for layout in "${layouts[@]}"; do
    for custom_query in "" "custom"; do
        if [[ $custom_query == custom ]]; then
            dictionary_name="dict_${layout}_custom"
            dictionary_source="query \$doc\$SELECT key, value, last_insert_time FROM $CLICKHOUSE_DATABASE.table_for_update_field_dictionary WHERE {condition};\$doc\$ update_field 'last_insert_time'"

            echo "$layout/custom"
        else
            dictionary_name="dict_${layout}"
            dictionary_source="table 'table_for_update_field_dictionary' update_field 'last_insert_time'"

            echo "$layout"
        fi

        $CLICKHOUSE_CLIENT -nm -q "
            TRUNCATE TABLE table_for_update_field_dictionary;

            CREATE DICTIONARY $dictionary_name
            (
                key UInt64,
                value String,
                last_insert_time DateTime
            )
            PRIMARY KEY key
            SOURCE(CLICKHOUSE($dictionary_source))
            LAYOUT($layout())
            LIFETIME(1);

            -- { echoOn }
            INSERT INTO table_for_update_field_dictionary VALUES (1, 'First', now());
            SELECT key, value FROM $dictionary_name ORDER BY key ASC;

            INSERT INTO table_for_update_field_dictionary VALUES (2, 'Second', now());
            SELECT sleepEachRow(1) FROM numbers(10) SETTINGS function_sleep_max_microseconds_per_block = 10000000 FORMAT Null;

            SELECT key, value FROM $dictionary_name ORDER BY key ASC;

            INSERT INTO table_for_update_field_dictionary VALUES (2, 'SecondUpdated', now());
            INSERT INTO table_for_update_field_dictionary VALUES (3, 'Third', now());
            SELECT sleepEachRow(1) FROM numbers(20) SETTINGS function_sleep_max_microseconds_per_block = 20000000 FORMAT Null;

            SELECT key, value FROM $dictionary_name ORDER BY key ASC;
            -- { echoOff }

            DROP DICTIONARY $dictionary_name;
        "

    done
done

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS table_for_update_field_dictionary"
