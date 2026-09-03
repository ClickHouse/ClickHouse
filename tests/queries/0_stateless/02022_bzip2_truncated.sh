#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

tmp_path=$(mktemp "$CURDIR/02022_bzip2_truncate.XXXXXX.bz2")
trap 'rm -f $tmp_path' EXIT

${CLICKHOUSE_LOCAL} -q "SELECT * FROM numbers(1e6) FORMAT TSV" | bzip2 > "$tmp_path"
truncate -s10000 "$tmp_path"
# just ensure that it will exit eventually
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('$tmp_path', 'TSV', 'n UInt64') FORMAT Null" >& /dev/null

exit 0
