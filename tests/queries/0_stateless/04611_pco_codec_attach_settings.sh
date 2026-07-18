#!/usr/bin/env bash
# Tags: no-fasttest
# The experimental PCO (pcodec) codec requires a column type and is rejected in the untyped MergeTree
# compression settings (`marks_compression_codec`, `primary_key_compression_codec`,
# `default_compression_codec`). That rejection only runs from `MergeTreeSettings::sanityCheck`, which
# `MergeTreeData` skips on the metadata-load path (ATTACH / SECONDARY_CREATE / RESTORE). So a table
# whose stored metadata carries such a setting still loads and would only fail later, at the first
# write. Check that the setting is normalized to the default codec on load, so the table stays writable.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04611_pco_codec_attach_settings"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/local"

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/metadata/local.sql"

# `marks_compression_codec` compresses the marks directly (always untyped).
cat > "${WORKING_FOLDER}/metadata/local/t_marks.sql" <<EOF
ATTACH TABLE local.t_marks (x UInt32) ENGINE=MergeTree ORDER BY tuple() SETTINGS marks_compression_codec='PCO';
EOF

# `primary_key_compression_codec` compresses the primary key directly (always untyped).
cat > "${WORKING_FOLDER}/metadata/local/t_pk.sql" <<EOF
ATTACH TABLE local.t_pk (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id SETTINGS primary_key_compression_codec='PCO';
EOF

# `default_compression_codec` becomes the part default codec, which is fed raw (untyped) into the
# statistics stream.
cat > "${WORKING_FOLDER}/metadata/local/t_def.sql" <<EOF
ATTACH TABLE local.t_def (id UInt64, v Int64 STATISTICS(tdigest)) ENGINE=MergeTree ORDER BY id SETTINGS default_compression_codec='PCO';
EOF

# Each table loads through the ATTACH path (sanity checks skipped); the first write must succeed, not
# throw `Codec 'PCO' was created without a numeric column type and cannot compress`.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SET allow_experimental_statistics = 1;
INSERT INTO local.t_marks SELECT number FROM numbers(1000);
SELECT 'marks', count() FROM local.t_marks;
INSERT INTO local.t_pk SELECT number, number FROM numbers(1000);
SELECT 'primary_key', count() FROM local.t_pk;
INSERT INTO local.t_def SELECT number, number FROM numbers(1000);
SELECT 'default', count() FROM local.t_def;
"

rm -rf "${WORKING_FOLDER}"
