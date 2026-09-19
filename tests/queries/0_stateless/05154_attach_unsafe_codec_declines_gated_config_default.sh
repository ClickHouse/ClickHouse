#!/usr/bin/env bash
# Tags: no-fasttest
# The value the metadata-load path restores for an unsafe untyped compression-codec setting comes from
# the `<merge_tree>` config defaults, which are not stored in the table metadata. Such a value is only
# usable while the codec gate of the default profile allows it - that is the policy a config-inherited
# codec is validated against on every load, and the only one that survives a restart. A gated config
# default must therefore be declined and the declaration default used instead, so that resetting the
# unsafe setting cannot make the table start writing with a codec nobody enabled.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/05154_attach_unsafe_codec_declines_gated_config_default"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/data/metadata/local"

# `ZXC` is experimental: it needs `enable_zxc_codec`, which the default profile here does not set.
cat > "${WORKING_FOLDER}/config.xml" <<CONFIG
<clickhouse>
    <merge_tree>
        <default_compression_codec>ZXC</default_compression_codec>
    </merge_tree>
</clickhouse>
CONFIG

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/data/metadata/local.sql"

# `T64` needs the column type, so it can never compress the untyped streams a part default codec is
# fed into; the stored setting is therefore reset on load. It is stored in the definition, so the
# config default is not validated on this load and the table still attaches.
cat > "${WORKING_FOLDER}/data/metadata/local/t_def.sql" <<METADATA
ATTACH TABLE local.t_def (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id SETTINGS default_compression_codec='T64';
METADATA

${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/config.xml" --path="${WORKING_FOLDER}/data" --multiquery "
INSERT INTO local.t_def (id, v) SELECT number, number FROM numbers(1000);
SELECT 'rows', count() FROM local.t_def;
SELECT 'part_codec_is_gated', default_compression_codec LIKE '%ZXC%'
FROM system.parts WHERE database = 'local' AND table = 't_def' AND active;
"

rm -rf "${WORKING_FOLDER}"
