#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-local` does not run a `ConfigReloader`, so the `max_*_num_to_throw`
# entity limits are enforced only because their mirrors are seeded in
# `Context::setApplicationType`. This is a focused regression test for that path:
# a change that stops seeding the mirrors would silently disable enforcement in
# `clickhouse-local`, while the server-based integration test would keep passing
# (the server re-seeds the same mirrors from its config-reloader callback).

config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_config.xml"

cat > "$config" <<'EOF'
<clickhouse>
    <max_table_num_to_throw>1</max_table_num_to_throw>
</clickhouse>
EOF

# Under the limit a single table can be created.
${CLICKHOUSE_LOCAL} --config-file="$config" --query "CREATE TABLE t (x UInt8) ENGINE = Memory; SELECT 'one table is ok';"

# Creating a second table exceeds the limit and must throw `TOO_MANY_TABLES`.
${CLICKHOUSE_LOCAL} --config-file="$config" --query "
CREATE TABLE t1 (x UInt8) ENGINE = Memory;
CREATE TABLE t2 (x UInt8) ENGINE = Memory;
" 2>&1 | grep -o -m1 'TOO_MANY_TABLES'

rm -f "$config"
