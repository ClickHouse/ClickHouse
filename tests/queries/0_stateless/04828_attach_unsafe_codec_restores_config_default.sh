#!/usr/bin/env bash
# Tags: no-fasttest
# When the metadata-load path (ATTACH) resets an untyped-unsafe compression-codec setting (e.g.
# `default_compression_codec = 'PCO'`), the replacement must be the pre-override effective value —
# the current `<merge_tree>` config default — not the declaration default. The stored entry is
# dropped from the table metadata, so the next load resolves the setting from the config; if the
# running table had been reset to the declaration default instead, it would write with a different
# codec until the next reload.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04828_attach_unsafe_codec_restores_config_default"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/data/metadata/local"

# The config overrides the default part codec, so the effective "no override" value is ZSTD(7),
# not the declaration default (empty, which resolves to LZ4).
cat > "${WORKING_FOLDER}/config.xml" <<EOF
<clickhouse>
    <merge_tree>
        <default_compression_codec>ZSTD(7)</default_compression_codec>
    </merge_tree>
</clickhouse>
EOF

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/data/metadata/local.sql"

cat > "${WORKING_FOLDER}/data/metadata/local/t_def.sql" <<EOF
ATTACH TABLE local.t_def (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id SETTINGS default_compression_codec='PCO';
EOF

# The unsafe stored setting is reset on load; the part written right after must already use the
# config default, and the reset must be durable — an unrelated `ALTER` re-parses the stored
# `settings_changes` (re-running the sanity check), so it would be rejected if the unsafe codec
# were still advertised there.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/config.xml" --path="${WORKING_FOLDER}/data" --multiquery "
INSERT INTO local.t_def (id, v) SELECT number, number FROM numbers(1000);
SELECT 'first_load', count() FROM local.t_def;
SELECT 'first_load_part_codec', default_compression_codec FROM system.parts WHERE database = 'local' AND table = 't_def' AND active;
ALTER TABLE local.t_def ADD COLUMN w UInt32;
SELECT 'after_alter', count() FROM local.t_def;
"

# On the next load the setting resolves from the config; a part written now must use the same codec
# as the one written on the first load — the running table and the reloaded table agree.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/config.xml" --path="${WORKING_FOLDER}/data" --multiquery "
INSERT INTO local.t_def (id, v) SELECT number, number FROM numbers(1000);
SELECT 'second_load', count() FROM local.t_def;
SELECT 'part_codecs_agree', groupUniqArray(default_compression_codec) FROM system.parts WHERE database = 'local' AND table = 't_def' AND active;
"

rm -rf "${WORKING_FOLDER}"
