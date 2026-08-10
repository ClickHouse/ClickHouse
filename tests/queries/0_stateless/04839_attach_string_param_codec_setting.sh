#!/usr/bin/env bash
# Tags: no-fasttest
# A stored codec-valued setting whose codec takes a string parameter, e.g.
# `default_compression_codec = 'T64(''bit'')'`, has to be classified on the metadata-load path (and
# rejected at `CREATE` time) as "requires a column type", not fail while being parsed. Normalizing the
# codec string by upper-casing all of it rewrites the literal into `T64('BIT')`, which the codec rejects
# with `Wrong modification for T64`, so a short `ATTACH` would fail to load the table instead of
# sanitizing the setting.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04839_attach_string_param_codec_setting"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/data/metadata/local"

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/data/metadata/local.sql"

cat > "${WORKING_FOLDER}/data/metadata/local/t_t64.sql" <<EOF
ATTACH TABLE local.t_t64 (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id SETTINGS default_compression_codec='T64(\\'bit\\')';
EOF

# `T64` needs a column type, so the setting is unsafe for the untyped streams the part default codec is
# used for and the load resets it instead of failing: the table loads and the part written right after
# uses the default codec.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}/data" --multiquery "
INSERT INTO local.t_t64 (id, v) SELECT number, number FROM numbers(100);
SELECT 'loaded', count() FROM local.t_t64;
SELECT 'part_codec', default_compression_codec FROM system.parts WHERE database = 'local' AND table = 't_t64' AND active;
"

# The same codec string as fresh user input is rejected for the reason it is unusable, not with a parse
# error about the rewritten literal.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_t64_fresh (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS default_compression_codec = 'T64(''bit'')'" 2>&1 | grep -o -m1 -E "Codec T64 requires the column type|Wrong modification for T64"

# The typed path is unaffected: a column codec with a string parameter still works.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_t64_column"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_t64_column (id UInt64 CODEC(T64('bit'))) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_t64_column SELECT number FROM numbers(100)"
${CLICKHOUSE_CLIENT} --query "SELECT 'column_codec', count() FROM t_t64_column"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_t64_column"

rm -rf "${WORKING_FOLDER}"
