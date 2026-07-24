#!/usr/bin/env bash
# When the metadata-load path resets an unsafe compression-codec setting (see
# 04611_pco_codec_attach_settings), the reset is also applied to the stored `settings_changes` AST.
# If the unsafe codec was the only stored setting, the AST must be dropped entirely: a kept-but-empty
# settings list would make the next metadata rewrite (e.g. by `ALTER`) emit a bare `SETTINGS` clause,
# producing unparseable `SHOW CREATE` / backup metadata and a table that cannot be loaded again.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04645_codec_settings_sanitize_last_setting"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/local"

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/metadata/local.sql"

# `T64` cannot compress untyped data, so the setting is reset on load; it is the only stored setting.
cat > "${WORKING_FOLDER}/metadata/local/t_only.sql" <<EOF
ATTACH TABLE local.t_only (x UInt32) ENGINE=MergeTree ORDER BY tuple() SETTINGS marks_compression_codec='T64';
EOF

# Here the codec is not the only stored setting: the other setting must survive the reset.
cat > "${WORKING_FOLDER}/metadata/local/t_two.sql" <<EOF
ATTACH TABLE local.t_two (x UInt32) ENGINE=MergeTree ORDER BY tuple() SETTINGS marks_compression_codec='T64', index_granularity=555;
EOF

# The ALTER rewrites the stored metadata from the sanitized in-memory metadata.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
INSERT INTO local.t_only SELECT number FROM numbers(1000);
ALTER TABLE local.t_only ADD COLUMN y UInt32;
SELECT 'only', count() FROM local.t_only;
INSERT INTO local.t_two SELECT number FROM numbers(1000);
ALTER TABLE local.t_two ADD COLUMN y UInt32;
SELECT 'two', count() FROM local.t_two;
"

# The rewritten metadata must not contain a bare SETTINGS clause (a settings list emptied by the
# reset but still formatted).
grep -c "SETTINGS[[:space:]]*$" "${WORKING_FOLDER}/metadata/local/t_only.sql" "${WORKING_FOLDER}/metadata/local/t_two.sql" | sed "s|${WORKING_FOLDER}/metadata/local/||"

# The tables must load again from the rewritten metadata.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
SELECT 'only_after_reload', count() FROM local.t_only;
SELECT 'two_after_reload', count() FROM local.t_two;
SELECT 'two_granularity_kept', create_table_query LIKE '%index_granularity = 555%' FROM system.tables WHERE database = 'local' AND name = 't_two';
SELECT 'two_codec_dropped', create_table_query NOT LIKE '%marks_compression_codec%' FROM system.tables WHERE database = 'local' AND name = 't_two';
"

rm -rf "${WORKING_FOLDER}"
