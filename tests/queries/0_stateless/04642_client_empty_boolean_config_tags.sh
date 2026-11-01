#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The changelog advertises self-closing boolean tags (e.g. <echo/>, <highlight/>, <hints/>).
# Raw Poco boolean parsing throws on an empty value, so these reads must go through
# ConfigHelper::getBool, which treats the empty tag as `true`. This test guards every
# advertised empty-tag boolean against a regression back to raw `getBool`.

config=$CLICKHOUSE_TMP/empty_boolean_tags_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config}"
}
trap cleanup EXIT

echo "-- <echo/> empty tag enables echoing in batch mode"
cat > "$config" <<'EOF'
<config>
    <echo/>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT 1"

echo "-- <echo_formatted/> empty tag enables formatted echo (blank lines surround the query)"
cat > "$config" <<'EOF'
<config>
    <echo_formatted/>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" --echo -q "SELECT 1"

echo "-- <echo_query_id/> empty tag prints the query ID line"
cat > "$config" <<'EOF'
<config>
    <echo_query_id/>
</config>
EOF
query_id="test-query-${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --config "$config" --query-id "$query_id" -q "SELECT 1" | sed "s/${query_id}/test-query-123/"

echo "-- <highlight/> and <hints/> empty tags do not break the client (read in setupEchoAndHighlightSettings)"
cat > "$config" <<'EOF'
<config>
    <highlight/>
    <hints/>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT 1"

echo "-- <enable_progress_table_toggle/> empty tag is accepted by the underscore remap"
cat > "$config" <<'EOF'
<config>
    <enable_progress_table_toggle/>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT 1"
