#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy defined on a view reached through a read-only Overlay facade must keep applying even
# when the analyzer inlines the view (`analyzer_inline_views = 1`). Inlining replaces the view's
# TableNode with a subquery built from the source view's id, so the facade's own row policies must
# be combined in at inlining time — otherwise they are silently dropped and the facade filter is
# bypassed.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
USER="u_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.base (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.base VALUES (1), (2), (3), (4);
    CREATE VIEW ${DB_SRC}.v AS SELECT id FROM ${DB_SRC}.base;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER} NOT IDENTIFIED;
    GRANT SELECT ON ${DB_OVL}.* TO ${USER};
    GRANT SELECT ON ${DB_SRC}.* TO ${USER};

    -- A row policy on the facade view only (the source view has none).
    CREATE ROW POLICY p_ovl_${SUF} ON ${DB_OVL}.v AS permissive FOR SELECT USING id <= 2 TO ${USER};
"

read_overlay() { ${CLICKHOUSE_CLIENT} --user="${USER}" --query "SELECT id FROM ${DB_OVL}.v ORDER BY id SETTINGS enable_analyzer = 1, analyzer_inline_views = $1" | xargs; }

echo "analyzer_inline_views=1: the facade row policy still applies after view inlining (expect 1 2)"
read_overlay 1
echo "analyzer_inline_views=0: the facade row policy applies without inlining (expect 1 2)"
read_overlay 0

echo "direct reads of the source view are unaffected by the facade policy (expect 1 2 3 4)"
${CLICKHOUSE_CLIENT} --user="${USER}" --query "SELECT id FROM ${DB_SRC}.v ORDER BY id SETTINGS enable_analyzer = 1, analyzer_inline_views = 1" | xargs

${CLICKHOUSE_CLIENT} -nm --query "
    DROP ROW POLICY IF EXISTS p_ovl_${SUF} ON ${DB_OVL}.v;
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER};
"
