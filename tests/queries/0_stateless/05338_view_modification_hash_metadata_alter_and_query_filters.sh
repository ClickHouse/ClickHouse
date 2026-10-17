#!/usr/bin/env bash
# Tags: no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The `modification_hash` of a view must not move on a metadata `ALTER` that cannot change its rows,
# such as `MODIFY COMMENT`, and must ignore the `additional_table_filters` entries in the `SETTINGS`
# of the stored query that are keyed by a table the view does not read.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    SYSTEM STOP MERGES src;
    INSERT INTO src VALUES (1), (2);
    CREATE VIEW v AS SELECT x FROM src;
"

hash_of_v()
{
    $CLICKHOUSE_CLIENT -q "SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v'"
}

before=$(hash_of_v)
[ -n "$before" ] && [ "$before" != "\\N" ] && echo "view has a hash: yes" || echo "view has a hash: no ($before)"

$CLICKHOUSE_CLIENT -q "ALTER TABLE v MODIFY COMMENT 'c_05338'"
commented=$(hash_of_v)
[ "$before" = "$commented" ] && echo "a comment keeps the hash: yes" || echo "a comment keeps the hash: no"

# The hash of a view folds its UUID, so recreate one view under the same UUID with another query.
# A `Replicated` database rejects an explicit UUID unless `database_replicated_allow_explicit_uuid` allows it,
# and warns about it even then, so do not let the warning reach the stderr.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
hash_of_query()
{
    $CLICKHOUSE_CLIENT --database_replicated_allow_explicit_uuid 1 --send_logs_level error -q "
        CREATE VIEW v1 UUID '${uuid}' AS $1;
        SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v1';
        DROP VIEW v1 SYNC;
    "
}
plain=$(hash_of_query "SELECT x FROM src")
unrelated=$(hash_of_query "SELECT x FROM src SETTINGS additional_table_filters = {'other_table_05338': 'x > 1'}")
related=$(hash_of_query "SELECT x FROM src SETTINGS additional_table_filters = {'src': 'x > 1'}")
mixed=$(hash_of_query "SELECT x FROM src SETTINGS additional_table_filters = {'src': 'x > 1', 'other_table_05338': 'x > 1'}")
[ -n "$plain" ] && [ "$plain" = "$unrelated" ] && echo "a filter for another table keeps the hash: yes" || echo "a filter for another table keeps the hash: no ($plain, $unrelated)"
[ "$plain" != "$related" ] && echo "a filter for the read table moves the hash: yes" || echo "a filter for the read table moves the hash: no"
[ "$related" = "$mixed" ] && echo "only the matching entries take part: yes" || echo "only the matching entries take part: no ($related, $mixed)"

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v SYNC;
    DROP TABLE src SYNC;
"
