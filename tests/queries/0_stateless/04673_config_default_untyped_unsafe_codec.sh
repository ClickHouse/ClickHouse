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

# The same holds for a lossy codec (`SZ3`): it can only be applied to a floating-point column, so it
# can never work on the untyped mark / primary key / part-default streams. Unlike `T64` it is not
# rejected by the `requiresColumnTypeToCompress` check, and the codec-valued settings are validated
# with the suspicious-codec sanity checks disabled, so it needs its own guard.
cat > "${WORKING_FOLDER}/unsafe_lossy.xml" <<EOF
<clickhouse>
    <merge_tree>
        <marks_compression_codec>SZ3</marks_compression_codec>
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

# `SZ3` is experimental, so it has to be enabled first - otherwise it never reaches the lossy check.
# `clickhouse-local` does not apply `<profiles>`, so the opt-in goes on the command line; the codec
# then passes the experimental gate and must still be rejected for being lossy.
SZ3_OPT_IN=(--allow_experimental_codecs=1 --enable_sz3_codec=1)

${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/unsafe_lossy.xml" "${SZ3_OPT_IN[@]}" --multiquery "
CREATE TABLE t_inherited_lossy (x UInt32) ENGINE = MergeTree ORDER BY tuple();
" 2>&1 | grep -o -m1 'cannot use the codec SZ3 because it is lossy'

# An explicit safe override keeps working, and `RESET SETTING` - which puts the lossy config default
# back - is rejected.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/unsafe_lossy.xml" "${SZ3_OPT_IN[@]}" --multiquery "
CREATE TABLE t_override_lossy (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'LZ4';
INSERT INTO t_override_lossy SELECT number FROM numbers(1000);
SELECT 'override_lossy', count() FROM t_override_lossy;
ALTER TABLE t_override_lossy RESET SETTING marks_compression_codec; -- { serverError BAD_ARGUMENTS }
"

${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/safe.xml" "${SZ3_OPT_IN[@]}" --multiquery "
CREATE TABLE t_modify_lossy (x UInt32) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_modify_lossy MODIFY SETTING marks_compression_codec = 'SZ3';
" 2>&1 | grep -o -m1 'cannot use the codec SZ3 because it is lossy'

# A safe config default keeps working, both on CREATE and after a RESET SETTING.
${CLICKHOUSE_LOCAL} --config-file="${WORKING_FOLDER}/safe.xml" --multiquery "
CREATE TABLE t_safe (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'LZ4';
ALTER TABLE t_safe RESET SETTING marks_compression_codec;
INSERT INTO t_safe SELECT number FROM numbers(1000);
SELECT 'safe', count() FROM t_safe;
"

rm -rf "${WORKING_FOLDER}"
