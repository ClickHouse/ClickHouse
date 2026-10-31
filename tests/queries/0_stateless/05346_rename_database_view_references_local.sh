#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE in clickhouse-local. The server cases are in 05342 and 05344.

echo "-- clickhouse-local, a view in the default (Overlay) database"
LOCAL_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
rm -rf "${LOCAL_DIR}"
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "
    CREATE DATABASE a ENGINE = Atomic;
    CREATE TABLE a.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE default.d (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW default.v AS SELECT x FROM a.t;
    CREATE MATERIALIZED VIEW default.mv TO default.d AS SELECT x FROM a.t;
    RENAME DATABASE a TO b;
    INSERT INTO b.t VALUES (1);
    SELECT count() FROM default.v;
    SELECT count() FROM default.d;
"
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "
    INSERT INTO b.t VALUES (2);
    SELECT count() FROM default.v;
    SELECT count() FROM default.d;
    SELECT countIf(create_table_query LIKE '% a.%') FROM system.tables WHERE database = 'default' AND name IN ('v', 'mv');
"
rm -rf "${LOCAL_DIR}"

echo "-- table function arguments that the rename cannot rewrite"
# Each refusal names the first affected view, so dropping it shows the next one. Any other error is printed as is.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --ignore-error -m -q "
    CREATE DATABASE f ENGINE = Atomic;
    CREATE DATABASE ext ENGINE = Atomic;
    CREATE TABLE f.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO f.t VALUES (1);
    CREATE VIEW f.p AS SELECT {n:UInt64} AS x;
    CREATE VIEW ext.index_v AS SELECT x FROM loop(mergeTreeIndex(f, t)) LIMIT 1;
    CREATE VIEW ext.loop_v AS SELECT * FROM loop('f', 't') LIMIT 1;
    CREATE VIEW ext.qualified_loop_v AS SELECT * FROM loop(f.t) LIMIT 1;
    CREATE VIEW ext.param_v AS SELECT * FROM f.p(n = 1);
    CREATE VIEW ext.file_v AS SELECT * FROM file('f', 'CSV', 'x UInt64');
    RENAME DATABASE f TO g;
    DROP VIEW ext.index_v;
    RENAME DATABASE f TO g;
    DROP VIEW ext.loop_v;
    RENAME DATABASE f TO g;
    DROP VIEW ext.qualified_loop_v;
    RENAME DATABASE f TO \`g.dot\`;
    SELECT * FROM ext.param_v;
    RENAME DATABASE f TO g;
    SELECT * FROM ext.param_v;
" > "${LOCAL_DIR}.out" 2> "${LOCAL_DIR}.err"
sed -n -e "s/.*because ext\.\([a-z_]*\) refers to it in the table function .*(\([A-Z_]*\)).*/\1: \2/p; t" -e "/Code: /p" "${LOCAL_DIR}.err"
cat "${LOCAL_DIR}.out"
rm -rf "${LOCAL_DIR}" "${LOCAL_DIR}.out" "${LOCAL_DIR}.err"
