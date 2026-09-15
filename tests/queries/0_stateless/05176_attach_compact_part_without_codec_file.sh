#!/usr/bin/env bash
# An all-default-codec Compact part that lacks `default_compression_codec.txt` could not be attached:
# the codec proof looked for the per-column stream files that a Compact part does not have (only a
# column literally named `data` resolved, by name coincidence, to the shared `data.bin`), so it fell
# back to reading `checksums.txt` as a codec frame and refused the part. `clickhouse local` is used
# because the test removes a file from a part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05176_${CLICKHOUSE_DATABASE}"
rm -rf "${workdir}"
mkdir -p "${workdir}"

drop_codec_file() {
    find "${workdir}/store" -path "*detached/$1/default_compression_codec.txt" -delete
}

for part_type in compact wide
do
    if [[ "${part_type}" == "compact" ]]; then min_bytes_for_wide_part=1000000; else min_bytes_for_wide_part=0; fi

    ${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
    CREATE TABLE t_${part_type} (key UInt64, payload String) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = ${min_bytes_for_wide_part};
    INSERT INTO t_${part_type} VALUES (1, 'Hello world');
    SELECT '${part_type}', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_${part_type}' AND active;
    ALTER TABLE t_${part_type} DETACH PART 'all_1_1_0';
    "

    drop_codec_file all_1_1_0

    ${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
    ALTER TABLE t_${part_type} ATTACH PART 'all_1_1_0';
    SELECT 'attached', key, payload FROM t_${part_type};
    "
done

rm -rf "${workdir}"
