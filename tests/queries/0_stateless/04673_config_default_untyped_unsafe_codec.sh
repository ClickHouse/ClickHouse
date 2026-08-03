#!/usr/bin/env bash
# Tags: no-fasttest
# The untyped `MergeTree` compression settings (`marks_compression_codec`,
# `primary_key_compression_codec`, `default_compression_codec`) can also get their value from the
# `<merge_tree>` config defaults instead of the table definition. Such a value is not stored in the
# table metadata, so it is re-resolved on every load, and it must be validated exactly like an
# explicit one: a codec that can never work on an untyped stream (`T64`, which stores the column
# type id in the stream) has to be rejected even when `allow_experimental_codecs` is enabled
# everywhere, because that gate has nothing to do with this class of codecs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04673_config_default_untyped_unsafe_codec"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# `allow_experimental_codecs` is on in the default profile as well as in every session below, so
# nothing here is gated by experimentality; `T64` is not experimental in the first place.
cat > "${WORKING_FOLDER}/unsafe.xml" <<EOF
<clickhouse>
    <merge_tree>
        <marks_compression_codec>T64</marks_compression_codec>
    </merge_tree>
    <profiles>
        <default>
            <allow_experimental_codecs>1</allow_experimental_codecs>
        </default>
    </profiles>
</clickhouse>
EOF

cat > "${WORKING_FOLDER}/safe.xml" <<EOF
<clickhouse>
    <merge_tree>
        <marks_compression_codec>ZSTD(4)</marks_compression_codec>
    </merge_tree>
    <profiles>
        <default>
            <allow_experimental_codecs>1</allow_experimental_codecs>
        </default>
    </profiles>
</clickhouse>
EOF

# A table inheriting the unsafe config default must be rejected at CREATE, not accepted and then
# failed at the first mark write.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/unsafe.xml" --multiquery "
CREATE TABLE t_inherited (x UInt32) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
"
echo 'create_inherited_rejected'

# An explicit safe override makes the config default irrelevant, and `RESET SETTING` puts the
# unsafe config default back, so it must be rejected too.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/unsafe.xml" --multiquery "
CREATE TABLE t_override (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'LZ4';
INSERT INTO t_override SELECT number FROM numbers(1000);
SELECT 'override', count() FROM t_override;
ALTER TABLE t_override RESET SETTING marks_compression_codec; -- { serverError BAD_ARGUMENTS }
"

# A safe config default keeps working, both on CREATE and after a RESET SETTING.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/safe.xml" --multiquery "
CREATE TABLE t_safe (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'LZ4';
ALTER TABLE t_safe RESET SETTING marks_compression_codec;
INSERT INTO t_safe SELECT number FROM numbers(1000);
SELECT 'safe', count() FROM t_safe;
"

rm -rf "${WORKING_FOLDER}"
