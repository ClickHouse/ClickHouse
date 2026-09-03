#!/usr/bin/env bash
# Tags: no-fasttest
# When a part has no (or an unparsable) `default_compression_codec.txt`, `loadDefaultCompressionCodec`
# falls back to `detectDefaultCompressionCodec`, which rebuilds the codec from a data file's method
# bytes (`getCompressionCodecForFile`). Such a detected codec can be lossy — e.g. a column that was
# compressed with `SZ3` while it had an explicit `CODEC(SZ3)` in the table definition, before the
# definition changed and the codec file was lost (an attached or pre-fix part). A lossy part default
# is unsafe: a mutation copies it into the writer of the new part and feeds it raw into untyped
# streams (statistics, text indexes), where it either throws or silently corrupts the opaque bytes.
# Check that the metadata-load guard sanitizes a detected lossy codec — both a bare `SZ3` frame and a
# `Multiple` frame whose chain contains `SZ3` — the same way it sanitizes a codec that requires a
# column type, and that a statistics-rewriting mutation then succeeds.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04642_lossy_codec_part_default_detected_from_file"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/local"

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/metadata/local.sql"

# `v` comes first so `detectDefaultCompressionCodec` probes its data file first once the explicit
# codec is gone from the table definition.
cat > "${WORKING_FOLDER}/metadata/local/t_detect.sql" <<EOF
ATTACH TABLE local.t_detect (v Float64 CODEC(SZ3), s Int64 STATISTICS(tdigest)) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
EOF

# The chain variant writes a Multiple frame; the detected codec is then a Multiple whose chain
# contains the lossy SZ3, which the guard must classify as lossy too.
cat > "${WORKING_FOLDER}/metadata/local/t_chain.sql" <<EOF
ATTACH TABLE local.t_chain (v Float64 CODEC(SZ3, LZ4), s Int64 STATISTICS(tdigest)) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
EOF

# Write one part per table; `v.bin` is compressed with SZ3 / Multiple(SZ3, LZ4).
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SET allow_experimental_statistics = 1;
INSERT INTO local.t_detect SELECT number / 7, number FROM numbers(1000);
INSERT INTO local.t_chain SELECT number / 7, number FROM numbers(1000);
"

# Turn the parts into the problematic shape: drop the explicit codec from the stored table
# definition (so `v` counts as a default-codec column for detection) and remove the codec file
# (it is not covered by `checksums.txt`, so the part stays consistent), forcing the
# detect-from-file path on the next load.
cat > "${WORKING_FOLDER}/metadata/local/t_detect.sql" <<EOF
ATTACH TABLE local.t_detect (v Float64, s Int64 STATISTICS(tdigest)) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
EOF
cat > "${WORKING_FOLDER}/metadata/local/t_chain.sql" <<EOF
ATTACH TABLE local.t_chain (v Float64, s Int64 STATISTICS(tdigest)) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
EOF
rm "${WORKING_FOLDER}"/data/local/t_detect/all_*/default_compression_codec.txt
rm "${WORKING_FOLDER}"/data/local/t_chain/all_*/default_compression_codec.txt

# On load the detected lossy default must be replaced with the table's normal default codec
# selection (LZ4 here), and a mutation that rewrites the untyped statistics stream with the part
# default codec must succeed. Before the fix the part default stayed SZ3 / Multiple(SZ3, LZ4) and
# the mutation failed to compress the statistics blob.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SET allow_experimental_statistics = 1;
SET mutations_sync = 1;
SELECT 'detect_codec', default_compression_codec FROM system.parts WHERE database = 'local' AND table = 't_detect' AND active;
SELECT 'chain_codec', default_compression_codec FROM system.parts WHERE database = 'local' AND table = 't_chain' AND active;
ALTER TABLE local.t_detect UPDATE s = s + 1 WHERE 1;
SELECT 'detect_after_mutation', count(), sum(s) FROM local.t_detect;
ALTER TABLE local.t_chain UPDATE s = s + 1 WHERE 1;
SELECT 'chain_after_mutation', count(), sum(s) FROM local.t_chain;
"

rm -rf "${WORKING_FOLDER}"
