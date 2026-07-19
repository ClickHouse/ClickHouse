#!/usr/bin/env bash
# Tags: no-fasttest
# The experimental PCO (pcodec) codec requires a column type, and the lossy SZ3 codec can only be
# applied to floating-point columns; both are rejected in the untyped MergeTree compression settings
# (`marks_compression_codec`, `primary_key_compression_codec`, `default_compression_codec`). That
# rejection only runs from `MergeTreeSettings::sanityCheck`, which `MergeTreeData` skips on the
# metadata-load path (ATTACH / SECONDARY_CREATE / RESTORE). So a table whose stored metadata carries
# such a setting still loads and would only fail later, at the first write. Check that the setting is
# normalized to the default codec on load, so the table stays writable.

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

# The same must hold for a lossy codec (`SZ3`): resolving it without a column type throws while
# building the codec, so `getReasonUnsafeForUntypedData` must classify it without going through that
# throwing path, otherwise the ATTACH would fail during metadata load instead of resetting the setting.
cat > "${WORKING_FOLDER}/metadata/local/t_marks_sz3.sql" <<EOF
ATTACH TABLE local.t_marks_sz3 (x UInt32) ENGINE=MergeTree ORDER BY tuple() SETTINGS marks_compression_codec='SZ3';
EOF
cat > "${WORKING_FOLDER}/metadata/local/t_pk_sz3.sql" <<EOF
ATTACH TABLE local.t_pk_sz3 (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id SETTINGS primary_key_compression_codec='SZ3';
EOF
cat > "${WORKING_FOLDER}/metadata/local/t_def_sz3.sql" <<EOF
ATTACH TABLE local.t_def_sz3 (id UInt64, v Int64 STATISTICS(tdigest)) ENGINE=MergeTree ORDER BY id SETTINGS default_compression_codec='SZ3';
EOF

# Each table loads through the ATTACH path (sanity checks skipped); the first write must succeed, not
# throw `Codec 'PCO' was created without a numeric column type and cannot compress` (nor the lossy
# `SZ3` error) after the unsafe setting is reset to the default codec. The reset must also be durable:
# resetting only the live settings leaves the stored `settings_changes` AST advertising the unsafe
# codec, so an unrelated `ALTER` that re-runs the sanity check on it would still be rejected. Run such
# an `ALTER ADD COLUMN` after the attach and require it to succeed.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SET allow_experimental_statistics = 1;
INSERT INTO local.t_marks SELECT number FROM numbers(1000);
SELECT 'marks', count() FROM local.t_marks;
ALTER TABLE local.t_marks ADD COLUMN y UInt32;
SELECT 'marks_after_alter', count() FROM local.t_marks;
INSERT INTO local.t_pk SELECT number, number FROM numbers(1000);
SELECT 'primary_key', count() FROM local.t_pk;
ALTER TABLE local.t_pk ADD COLUMN w UInt32;
SELECT 'primary_key_after_alter', count() FROM local.t_pk;
INSERT INTO local.t_def SELECT number, number FROM numbers(1000);
SELECT 'default', count() FROM local.t_def;
ALTER TABLE local.t_def ADD COLUMN w UInt32;
SELECT 'default_after_alter', count() FROM local.t_def;
INSERT INTO local.t_marks_sz3 SELECT number FROM numbers(1000);
SELECT 'marks_sz3', count() FROM local.t_marks_sz3;
INSERT INTO local.t_pk_sz3 SELECT number, number FROM numbers(1000);
SELECT 'primary_key_sz3', count() FROM local.t_pk_sz3;
INSERT INTO local.t_def_sz3 SELECT number, number FROM numbers(1000);
SELECT 'default_sz3', count() FROM local.t_def_sz3;
ALTER TABLE local.t_def_sz3 ADD COLUMN w UInt32;
SELECT 'default_sz3_after_alter', count() FROM local.t_def_sz3;
"

rm -rf "${WORKING_FOLDER}"
