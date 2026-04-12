#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

cat > "${tmp_dir}/tmp.csv" <<'EOF'
"id","str","int","text"
1,"abc",123,"abacaba"
2,"def",456,"bacabaa"
3,"story",78912,"acabaab"
4,"history",21321321,"cabaaba"
EOF

chmod 777 "${tmp_dir}"
chmod 777 "${tmp_dir}/tmp.csv"

${CLICKHOUSE_CLIENT} -q "SELECT COUNT(*) FROM file('${unique_name}/tmp.csv')"

rm -rf "${tmp_dir:?}"
