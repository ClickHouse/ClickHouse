#!/usr/bin/env bash
# The `convert_to_replicated` flag converted a table that has `table_readonly = 1` - a setting that is
# not supported for `ReplicatedMergeTree` - into a replicated table carrying it: exactly the state the
# check in the constructor exists to make unrepresentable. The conversion is skipped instead, so the
# table keeps serving and the setting can be reset; the flag stays, so the conversion runs once it is
# gone. `clickhouse local` is used because the flag is a file in the table's directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05192_${CLICKHOUSE_DATABASE}"
rm -rf "${workdir}"
mkdir -p "${workdir}"

# A readonly table refuses writes, so the rows are inserted before the setting is put on it.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t SELECT number FROM numbers(10);
ALTER TABLE t MODIFY SETTING table_readonly = 1;
SELECT 'rows', count() FROM t;
"

data_dir=$(dirname "$(find "${workdir}" -name format_version.txt | head -1)")
touch "${data_dir}/convert_to_replicated"

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'the engine is unchanged', engine FROM system.tables WHERE database = 'default' AND name = 't';
SELECT 'and the rows are readable', count() FROM t;
"

echo -n 'the flag is kept: '
[[ -f "${data_dir}/convert_to_replicated" ]] && echo 1 || echo 0

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
ALTER TABLE t RESET SETTING table_readonly;
SELECT 'the setting is gone from the table', countIf(create_table_query LIKE '%table_readonly%') FROM system.tables
WHERE database = 'default' AND name = 't';
"

rm -rf "${workdir}"
