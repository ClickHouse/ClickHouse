#!/usr/bin/env bash
# A part detached before a metadata-only `ALTER` and then missing its `metadata_version.txt` - a file
# the part checksums do not cover - used to be attached at the table's *current* version, so the
# pending `RENAME COLUMN` was skipped and every row of the renamed column read as its default. Such a
# part must be refused instead. `clickhouse local` is used because the test removes a file from a part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05171_${CLICKHOUSE_DATABASE}"
rm -rf "${workdir}"
mkdir -p "${workdir}"

drop_metadata_version() {
    find "${workdir}/store" -path "*detached/$1/metadata_version.txt" -delete
}

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
CREATE TABLE renamed (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO renamed SELECT number, number FROM numbers(1000);
SELECT 'before the rename', count(), sum(a) FROM renamed;
ALTER TABLE renamed DETACH PARTITION tuple();
ALTER TABLE renamed RENAME COLUMN a TO b;
"

drop_metadata_version all_1_1_0

echo -n 'attach refused: '
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "ALTER TABLE renamed ATTACH PARTITION tuple()" 2>&1 |
    grep -c -m1 'metadata_version.txt'

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'still detached', count() FROM system.detached_parts WHERE table = 'renamed';
SELECT 'nothing attached', count() FROM renamed;
"

# The same missing file over an unchanged schema: the part's columns match the table's, so reading it
# at the current version is the same as reading it at its own.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
CREATE TABLE unchanged (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO unchanged SELECT number, number FROM numbers(1000);
ALTER TABLE unchanged DETACH PARTITION tuple();
"

drop_metadata_version all_1_1_0

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
ALTER TABLE unchanged ATTACH PARTITION tuple();
SELECT 'unchanged schema attaches', count(), sum(a) FROM unchanged;
"

# And with a column dropped meanwhile: the part carries a column the table does not, but the table has
# nothing the part lacks, so the part is still readable as it is.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
CREATE TABLE dropped (id UInt64, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO dropped SELECT number, number, number FROM numbers(1000);
ALTER TABLE dropped DETACH PARTITION tuple();
ALTER TABLE dropped DROP COLUMN c;
"

drop_metadata_version all_1_1_0

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
ALTER TABLE dropped ATTACH PARTITION tuple();
SELECT 'dropped column attaches', count(), sum(a) FROM dropped;
"

rm -rf "${workdir}"
