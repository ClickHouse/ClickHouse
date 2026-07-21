#!/usr/bin/env bash
# Tags: no-fasttest
# An attached (or pre-fix) part may carry a codec in `default_compression_codec.txt` that cannot be
# used for untyped streams (the experimental `PCO` requires a column type). On load it is sanitized,
# and the sanitized value must follow the table's normal default codec selection (the
# `default_compression_codec` setting), not a hardcoded `LZ4`: a mutation copies the part default
# into its writer as the codec for every rewritten column without an explicit `CODEC(...)`, so a
# hardcoded fallback would silently move the table off its configured default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04616_pco_codec_part_default_codec_mutation"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# A wide part is required: the column-rewrite mutation path reuses the source part's default codec,
# while a compact part is fully rewritten with a freshly selected codec and would not exercise it.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE local ENGINE=Ordinary;
CREATE TABLE local.t (k UInt32, v UInt32) ENGINE=MergeTree ORDER BY k
    SETTINGS default_compression_codec='ZSTD(3)', min_bytes_for_wide_part=0, min_rows_for_wide_part=0;
INSERT INTO local.t SELECT number, number FROM numbers(1000);
SELECT 'initial', default_compression_codec FROM system.parts WHERE database='local' AND table='t' AND active;
"

# Simulate the bad metadata: overwrite the part's stored default codec with one that is unusable for
# untyped data. The file is not covered by the part checksums, matching what a pre-fix server could
# have written.
echo -n "CODEC(PCO)" > "${WORKING_FOLDER}"/data/local/t/all_*/default_compression_codec.txt

${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SELECT 'count', count() FROM local.t;
ALTER TABLE local.t UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync=1;
SELECT 'sum_after_update', sum(v) FROM local.t;
SELECT 'mutated_codec', default_compression_codec FROM system.parts WHERE database='local' AND table='t' AND active;
"

rm -rf "${WORKING_FOLDER}"
