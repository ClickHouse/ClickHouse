#!/usr/bin/env bash
# `system.engine_settings` has to report, for every engine of the `MergeTree` family, the rows of
# `system.merge_tree_settings` or `system.replicated_merge_tree_settings` - also where the server's baseline is not
# the compiled-in defaults, which is where the two could drift apart. The `<merge_tree>` and
# `<replicated_merge_tree>` sections set one setting differently, so each family has to match its own table and
# not the other; the second run adds the `compatibility` setting. clickhouse-local, since both are read once into
# a server-wide instance.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG="${CLICKHOUSE_TMP}/engine_settings_parity_config.xml"
cat > "$CONFIG" <<'EOF'
<clickhouse>
    <merge_tree>
        <merge_max_block_size>1234</merge_max_block_size>
        <max_suspicious_broken_parts>50</max_suspicious_broken_parts>
    </merge_tree>
    <replicated_merge_tree>
        <max_suspicious_broken_parts>77</max_suspicious_broken_parts>
    </replicated_merge_tree>
</clickhouse>
EOF

QUERY="
CREATE VIEW expected_rows AS
    SELECT e.engine AS engine, s.name AS name, s.value AS value, s.\`default\` AS \`default\`, s.changed AS changed,
        s.description AS description, s.min AS min, s.max AS max, s.disallowed_values AS disallowed_values,
        s.readonly AS readonly, s.type AS type, s.is_obsolete AS is_obsolete, s.tier AS tier, s.source AS source,
        s.alias_for AS alias_for
    FROM (SELECT name AS engine, startsWith(name, 'Replicated') AS replicated FROM system.table_engines
          WHERE endsWith(name, 'MergeTree') AND NOT startsWith(name, 'Shared')) AS e
    INNER JOIN (SELECT 0 AS replicated, * FROM system.merge_tree_settings
                UNION ALL
                SELECT 1 AS replicated, * FROM system.replicated_merge_tree_settings) AS s
    ON e.replicated = s.replicated;

CREATE VIEW engine_rows AS
    SELECT engine, name, value, \`default\`, changed, description, min, max, disallowed_values,
        readonly, type, is_obsolete, tier, source, alias_for
    FROM system.engine_settings
    WHERE endsWith(engine, 'MergeTree') AND NOT startsWith(engine, 'Shared');

SELECT 'the sections took effect', engine, value, source FROM engine_rows
WHERE engine IN ('MergeTree', 'ReplicatedMergeTree') AND name IN ('merge_max_block_size', 'max_suspicious_broken_parts')
ORDER BY engine, name;

SELECT 'rolled back by compatibility', startsWith(engine, 'Replicated') AS replicated,
    countIf(source = 'compatibility') > 0
FROM engine_rows GROUP BY replicated ORDER BY replicated;

SELECT 'only in system.engine_settings', engine, name
FROM (SELECT * FROM engine_rows EXCEPT SELECT * FROM expected_rows) ORDER BY engine, name;

SELECT 'only in the MergeTree settings tables', engine, name
FROM (SELECT * FROM expected_rows EXCEPT SELECT * FROM engine_rows) ORDER BY engine, name;
"

echo "-- config sections"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "$QUERY"

echo "-- config sections and compatibility"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --compatibility=23.3 -q "$QUERY"

rm -f "$CONFIG"
