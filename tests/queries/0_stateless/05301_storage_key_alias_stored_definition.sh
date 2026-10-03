#!/usr/bin/env bash
# A table that an older server stored with an alias in PRIMARY KEY, ORDER BY, SAMPLE BY, TTL, a skip index
# or a constraint still loads and can be altered: only a new definition is refused, including a full-definition
# ATTACH (05301_storage_key_alias_not_allowed covers CREATE and ALTER).
#
# Such a definition can no longer be written through SQL, so the stored metadata of tables
# created without the aliases is edited into the form the older server wrote.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (c0 Int64, c1 Int64, d Date, INDEX i c1 TYPE minmax) ENGINE = MergeTree PRIMARY KEY c0 ORDER BY (c0, c1) TTL d + INTERVAL 1 DAY;
INSERT INTO db.t VALUES (1, 1, '2100-01-01'), (2, 2, '2100-01-01');
CREATE TABLE db.t2 (c0 UInt64, d Date, c1 Int64 TTL d + INTERVAL 1 DAY, CONSTRAINT k CHECK c0 >= 0) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO db.t2 VALUES (1, '2100-01-01', 1);
"

metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t.sql')
sed -i 's/PRIMARY KEY c0/PRIMARY KEY (c0 AS a)/; s/ORDER BY (c0, c1)/ORDER BY (c0 AS x, c1)/; s/TTL d + /TTL (d AS e) + /; s/INDEX i c1 /INDEX i (c1 AS b) /' "${metadata_file}"
metadata_file2=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t2.sql')
sed -i 's/Int64 TTL d + /Int64 TTL (d AS e) + /; s/CHECK c0 >= 0/CHECK (c0 AS k0) >= 0/; s/^SAMPLE BY c0$/SAMPLE BY (c0 AS s)/' "${metadata_file2}"
# Without this the load below would run against an unmodified definition, i.e. assert nothing.
grep -c -F -e 'PRIMARY KEY (c0 AS a)' -e 'ORDER BY (c0 AS x, c1)' -e 'TTL (d AS e) + ' -e 'INDEX i (c1 AS b) ' "${metadata_file}"
grep -c -F -e 'Int64 TTL (d AS e) + ' -e 'CHECK (c0 AS k0) >= 0' -e 'SAMPLE BY (c0 AS s)' "${metadata_file2}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT count() FROM db.t;
DETACH TABLE db.t;
ATTACH TABLE db.t;
SELECT sorting_key, primary_key FROM system.tables WHERE database = 'db' AND name = 't';
DETACH TABLE db.t2;
ATTACH TABLE db.t2;
SELECT count(), sampling_key FROM db.t2, system.tables WHERE database = 'db' AND name = 't2' GROUP BY sampling_key;
-- An ALTER that leaves such a definition as it was is not refused.
ALTER TABLE db.t ADD INDEX IF NOT EXISTS i c1 TYPE minmax;
ALTER TABLE db.t MODIFY ORDER BY (c0 AS x, c1);
ALTER TABLE db.t2 ADD CONSTRAINT IF NOT EXISTS k CHECK c0 >= 0;
ALTER TABLE db.t2 MODIFY SAMPLE BY (c0 AS s);
ALTER TABLE db.t2 MODIFY COLUMN c1 Int32;
SELECT type FROM system.columns WHERE database = 'db' AND table = 't2' AND name = 'c1';
"

# The same definition is refused when it comes with the ATTACH query rather than from the stored metadata.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ATTACH TABLE db.t3 UUID '05301000-0000-0000-0000-000000000003' (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 AS a);
" 2>&1 | grep -c "Alias 'a' is not allowed in PRIMARY KEY"

rm -rf "${WORKING_DIR}"
