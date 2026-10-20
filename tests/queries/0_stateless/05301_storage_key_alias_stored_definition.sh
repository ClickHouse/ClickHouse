#!/usr/bin/env bash
# A table that an older server stored with an alias in PRIMARY KEY, ORDER BY, SAMPLE BY, TTL, a skip index
# (also through an ALIAS column it uses) or a constraint still loads and can be altered: only a new or changed definition is refused, including a
# full-definition ATTACH (05301_storage_key_alias_not_allowed covers CREATE and ALTER).
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
SET allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE db.t4 (d Date, c0 UInt64, c1 UInt64) ENGINE = MergeTree(d, (c0, c1), 8192);
INSERT INTO db.t4 VALUES ('2100-01-01', 1, 1);
CREATE TABLE db.t5 (c0 Int64, c1 Int64, a Int64 ALIAS c1 * 2) ENGINE = MergeTree ORDER BY c0 SETTINGS add_minmax_index_for_numeric_columns = 1;
"

metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t.sql')
sed -i 's/PRIMARY KEY c0/PRIMARY KEY (c0 AS a)/; s/ORDER BY (c0, c1)/ORDER BY (c0 AS x, c1)/; s/TTL d + /TTL (d AS e) + /; s/INDEX i c1 /INDEX i (c1 AS b) /' "${metadata_file}"
metadata_file2=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t2.sql')
sed -i 's/Int64 TTL d + /Int64 TTL (d AS e) + /; s/CHECK c0 >= 0/CHECK (c0 AS k0) >= 0/; s/^SAMPLE BY c0$/SAMPLE BY (c0 AS s)/' "${metadata_file2}"
metadata_file4=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t4.sql')
sed -i 's/MergeTree(d, (c0, c1), 8192)/MergeTree(d, (c0 AS x, c1), 8192)/' "${metadata_file4}"
metadata_file5=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t5.sql')
sed -i 's/ALIAS c1 \* 2/ALIAS (c1 * 2 AS k1)/' "${metadata_file5}"
# Without this the load below would run against an unmodified definition, i.e. assert nothing.
grep -c -F -e 'PRIMARY KEY (c0 AS a)' -e 'ORDER BY (c0 AS x, c1)' -e 'TTL (d AS e) + ' -e 'INDEX i (c1 AS b) ' "${metadata_file}"
grep -c -F -e 'Int64 TTL (d AS e) + ' -e 'CHECK (c0 AS k0) >= 0' -e 'SAMPLE BY (c0 AS s)' "${metadata_file2}"
grep -c -F -e 'MergeTree(d, (c0 AS x, c1), 8192)' "${metadata_file4}"
grep -c -F -e 'ALIAS (c1 * 2 AS k1)' "${metadata_file5}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT count() FROM db.t;
DETACH TABLE db.t;
ATTACH TABLE db.t;
SELECT sorting_key, primary_key FROM system.tables WHERE database = 'db' AND name = 't';
DETACH TABLE db.t2;
ATTACH TABLE db.t2;
SELECT count(), sampling_key FROM db.t2, system.tables WHERE database = 'db' AND name = 't2' GROUP BY sampling_key;
DETACH TABLE db.t4;
ATTACH TABLE db.t4;
SELECT count(), primary_key FROM db.t4, system.tables WHERE database = 'db' AND name = 't4' GROUP BY primary_key;
-- An ALTER that leaves such a definition as it was is not refused.
ALTER TABLE db.t ADD INDEX IF NOT EXISTS i c1 TYPE minmax;
ALTER TABLE db.t MODIFY ORDER BY (c0 AS x, c1);
ALTER TABLE db.t2 ADD CONSTRAINT IF NOT EXISTS k CHECK c0 >= 0;
ALTER TABLE db.t2 MODIFY SAMPLE BY (c0 AS s);
ALTER TABLE db.t2 MODIFY COLUMN c1 Int32;
SELECT type FROM system.columns WHERE database = 'db' AND table = 't2' AND name = 'c1';
-- Nor one that only renames a column the definition uses, or the column it belongs to.
ALTER TABLE db.t2 RENAME COLUMN d TO d2;
ALTER TABLE db.t2 RENAME COLUMN c1 TO c3;
ALTER TABLE db.t2 RENAME COLUMN IF EXISTS missing TO c3;
SELECT groupArray(name) FROM system.columns WHERE database = 'db' AND table = 't2';
ALTER TABLE db.t5 RENAME COLUMN a TO b;
SELECT arraySort(groupArray(name)) FROM system.data_skipping_indices WHERE database = 'db' AND table = 't5';
"

# An ALTER that changes such a definition, or writes another one, may not keep the alias in it.
for query in \
    "ALTER TABLE db.t MODIFY ORDER BY (c0, c1 AS x)" \
    "ALTER TABLE db.t MODIFY TTL (d AS e) + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1)) SETTINGS materialize_ttl_after_modify = 0" \
    "ALTER TABLE db.t2 ADD COLUMN c4 Int64 TTL (d2 AS e) + INTERVAL 2 DAY" \
    "ALTER TABLE db.t2 RENAME COLUMN c3 TO c5, ADD COLUMN c3 Int64 TTL (d2 AS e) + INTERVAL 1 DAY"
do
    $CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "${query}" 2>&1 | grep -c -E "Alias '(x|e)' is not allowed in (ORDER BY|TTL)"
done

# The same definition is refused when it comes with the ATTACH query rather than from the stored metadata.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ATTACH TABLE db.t3 UUID '05301000-0000-0000-0000-000000000003' (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 AS a);
" 2>&1 | grep -c "Alias 'a' is not allowed in PRIMARY KEY"

# Nor a rename that turns a later ADD COLUMN IF NOT EXISTS of the new name into a no-op.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ALTER TABLE db.t2 RENAME COLUMN IF EXISTS c3 TO c6, ADD COLUMN IF NOT EXISTS c6 Int64;
SELECT groupArray(name) FROM system.columns WHERE database = 'db' AND table = 't2';
"

# CREATE HYPOTHETICAL INDEX checks the body of a SQL UDF, as ALTER ... ADD INDEX does
# (here, because clickhouse-local keeps the UDF out of the server's global namespace).
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE FUNCTION alias_udf AS x -> (x * 1000 AS c9);
CREATE HYPOTHETICAL INDEX h ON db.t2 alias_udf(c0) TYPE minmax;
" 2>&1 | grep -c "Alias 'c9' is not allowed in INDEX"

# The same for the `SET` assignments of a `TTL ... GROUP BY` rule, which are not children of the `TTL` element.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE FUNCTION alias_udf_ttl AS x -> (x * 1000 AS c9);
CREATE TABLE db.t_ttl_udf (k UInt64, v UInt64, d DateTime) ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 DAY GROUP BY k SET v = max(alias_udf_ttl(v));
" 2>&1 | grep -c "Alias 'c9' is not allowed in TTL"

# CREATE HYPOTHETICAL INDEX reports an alias from the definition of an ALIAS column only to a user who may read
# that column and the columns it uses, also when the expression folds to a constant (`isNull` of a non-nullable column).
user="u_05301_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_alias_access (public UInt64, secret UInt64, hidden UInt64 ALIAS (secret AS confidential) * 1000 + confidential) ENGINE = MergeTree ORDER BY public;
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;
"
for grant in "" "SELECT(public)" "SELECT(hidden)" "SELECT(secret)"
do
    [ -z "${grant}" ] || $CLICKHOUSE_CLIENT -q "GRANT ${grant} ON ${CLICKHOUSE_DATABASE}.t_alias_access TO ${user}"
    for expression in "hidden" "(public, isNull(hidden))"
    do
        $CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL INDEX h ON ${CLICKHOUSE_DATABASE}.t_alias_access ${expression} TYPE minmax" 2>&1 \
            | grep -o -m1 -E "ACCESS_DENIED|Alias 'confidential' is not allowed in INDEX"
    done
done
$CLICKHOUSE_CLIENT -q "DROP USER ${user}; DROP TABLE t_alias_access;"

rm -rf "${WORKING_DIR}"
