#!/usr/bin/env bash

# Inside a materialized view query, the source table read by the nested interpreter of `merge`, `loop`
# or `executable` is the inserted block, like the rest of the view query. The database name is spelled
# out: `currentDatabase()` in the view query does not resolve to the test database during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS mv_merge;
DROP TABLE IF EXISTS mv_loop;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS dist_target;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number + 100 FROM numbers(10);

CREATE TABLE dist_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist AS dist_target ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, dist_target);

CREATE TABLE dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

-- 'dist' raises the merge read above FetchColumns, so 'src' is planned by a nested interpreter.
CREATE MATERIALIZED VIEW mv_merge TO dst AS
    SELECT 'merge' AS name, count() AS block_rows,
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x > 0) AS value
    FROM src;

CREATE MATERIALIZED VIEW mv_loop TO dst AS
    SELECT 'loop' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) WHERE x > 0 LIMIT 25)) AS value
    FROM src;

INSERT INTO src VALUES (1), (2), (3);

SELECT * FROM dst ORDER BY name;

DROP TABLE mv_merge;
DROP TABLE mv_loop;
DROP TABLE dst;
DROP TABLE dist;
DROP TABLE dist_target;
DROP TABLE src;
"

# The input query of `executable` needs `user_scripts_path`, which is a server config option.
WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05388_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT
mkdir -p "${WORK_DIR}/user_scripts" "${WORK_DIR}/data"

cat > "${WORK_DIR}/user_scripts/echo_rows.sh" <<'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r x; do
    printf '%s\n' "${x}"
done
SCRIPT
chmod +x "${WORK_DIR}/user_scripts/echo_rows.sh"

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" -m --query "
CREATE DATABASE db_05388;
CREATE TABLE db_05388.src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05388.src SELECT number + 100 FROM numbers(10);
CREATE TABLE db_05388.dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

CREATE MATERIALIZED VIEW db_05388.mv_executable TO db_05388.dst AS
    SELECT 'executable' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05388.src WHERE x > 0))) AS value
    FROM db_05388.src;

INSERT INTO db_05388.src VALUES (1), (2), (3);

SELECT * FROM db_05388.dst ORDER BY name;
"
