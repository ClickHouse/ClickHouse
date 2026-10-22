#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_like_pushdown.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# A plain `NOT NULL` `TEXT` column of a `STRICT` table is otherwise pushdown-safe. However, SQLite evaluates `LIKE`
# case-insensitively for ASCII by default and does not use backslash as the implicit escape character, unlike
# ClickHouse. Pushing these predicates down can therefore discard rows before ClickHouse re-filters the result.
sqlite3 "$DB_PATH" "
    CREATE TABLE t(s TEXT NOT NULL) STRICT;
    INSERT INTO t VALUES ('a_b'), ('aXb'), ('Abc'), ('abc'), ('zzz');
"

${CLICKHOUSE_LOCAL} --query="
    CREATE TABLE t (s String) ENGINE = SQLite('$DB_PATH', 't');

    SELECT 'Escaped underscore in LIKE is evaluated by ClickHouse:';
    SELECT s FROM t WHERE s LIKE 'a\_%' ORDER BY s;

    SELECT 'NOT LIKE remains case-sensitive:';
    SELECT s FROM t WHERE s NOT LIKE 'a%' ORDER BY s;
"

# Prove that `LIKE` and `NOT LIKE` stay local while another predicate on the same pushdown-safe column still
# reaches SQLite.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
    CREATE TABLE t (s String) ENGINE = SQLite('$DB_PATH', 't');
    SELECT s FROM t WHERE s LIKE 'a\_%' FORMAT Null;
    SELECT s FROM t WHERE s NOT LIKE 'a%' FORMAT Null;
    SELECT s FROM t WHERE s = 'abc' FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `s` FROM `t`( WHERE .*)?$'
