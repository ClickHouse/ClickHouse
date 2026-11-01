#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The underscore XML spellings <echo_query_id/>, <echo_formatted/>, and
# <enable_progress_table_toggle/> must work in `clickhouse-local` too, not only in
# `clickhouse-client`: the remap to the dashed keys the read sites use lives in
# `ClientBase::remapClientConfigurationAliases`, shared by both entry points.

config=$CLICKHOUSE_TMP/local_aliases_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config}"
}
trap cleanup EXIT

query_id="alias-test-${CLICKHOUSE_DATABASE}"

cat > "$config" <<'EOF'
<config>
    <echo_query_id/>
</config>
EOF
echo "-- echo_query_id from config prints the query id in batch clickhouse-local"
$CLICKHOUSE_LOCAL --config-file "$config" --query_id "$query_id" -q "SELECT 1" | sed "s/${query_id}/QUERY_ID/"

echo "-- the CLI flag still wins over the config"
$CLICKHOUSE_LOCAL --config-file "$config" --query_id "$query_id" --echo-query-id=0 -q "SELECT 1"

cat > "$config" <<'EOF'
<config>
    <echo/>
    <echo_formatted/>
</config>
EOF
echo "-- echo_formatted from config formats the echoed query"
$CLICKHOUSE_LOCAL --config-file "$config" -q "select 1 as x format Null"

cat > "$config" <<'EOF'
<config>
    <enable_progress_table_toggle>0</enable_progress_table_toggle>
</config>
EOF
echo "-- enable_progress_table_toggle from config is accepted"
$CLICKHOUSE_LOCAL --config-file "$config" -q "SELECT 1"
