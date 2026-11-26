#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "$CUR_DIR/tmp"
test_dir=$(mktemp -d "$CUR_DIR/tmp/04840_XXXXXX")
trap 'rm -r "$test_dir"' EXIT

check_merge_order()
{
    local config_file=$1
    local merge_dir=$2
    local config_dir="$test_dir/${config_file%.*}"

    mkdir "$config_dir"

    tee "$config_dir/$config_file" >/dev/null <<'EOF'
<clickhouse>
    <order>main</order>
</clickhouse>
EOF

    mkdir "$config_dir/$merge_dir"

    # Create the files in the opposite order from the expected merge order.
    tee "$config_dir/$merge_dir/2-last.xml" >/dev/null <<'EOF'
<clickhouse>
    <order replace="1">2</order>
</clickhouse>
EOF

    tee "$config_dir/$merge_dir/10-first.xml" >/dev/null <<'EOF'
<clickhouse>
    <order replace="1">10</order>
</clickhouse>
EOF

    "$CLICKHOUSE_BINARY" extract-from-config --config-file "$config_dir/$config_file" --key order
}

check_merge_order config.xml config.d
check_merge_order users.xml users.d

mkdir "$test_dir/legacy"

tee "$test_dir/legacy/config.xml" >/dev/null <<'EOF'
<clickhouse>
    <order>main</order>
</clickhouse>
EOF

mkdir "$test_dir/legacy/conf.d" "$test_dir/legacy/config.d"

tee "$test_dir/legacy/conf.d/99-override.xml" >/dev/null <<'EOF'
<clickhouse>
    <order replace="1">conf</order>
</clickhouse>
EOF

tee "$test_dir/legacy/config.d/00-base.xml" >/dev/null <<'EOF'
<clickhouse>
    <order replace="1">config</order>
</clickhouse>
EOF

"$CLICKHOUSE_BINARY" extract-from-config --config-file "$test_dir/legacy/config.xml" --key order

mkdir "$test_dir/legacy_users"

tee "$test_dir/legacy_users/users.xml" >/dev/null <<'EOF'
<clickhouse>
    <order>main</order>
</clickhouse>
EOF

mkdir "$test_dir/legacy_users/conf.d" "$test_dir/legacy_users/users.d"

tee "$test_dir/legacy_users/conf.d/99-override.xml" >/dev/null <<'EOF'
<clickhouse>
    <order replace="1">conf</order>
</clickhouse>
EOF

tee "$test_dir/legacy_users/users.d/00-base.xml" >/dev/null <<'EOF'
<clickhouse>
    <order replace="1">users</order>
</clickhouse>
EOF

"$CLICKHOUSE_BINARY" extract-from-config --config-file "$test_dir/legacy_users/users.xml" --key order
