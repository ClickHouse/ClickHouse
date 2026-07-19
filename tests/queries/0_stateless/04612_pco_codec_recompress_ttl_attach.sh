#!/usr/bin/env bash
# Tags: no-fasttest
# A `TTL ... RECOMPRESS CODEC(...)` codec is resolved with a null column type in
# `MergeTreeData::getCompressionCodecForPart`, so a codec that is unsafe for untyped data — experimental
# (e.g. `PCO`/`ALP`) or lossy (e.g. `SZ3`) — is rejected at `CREATE`. But `MergeTreeData` relaxes the codec
# checks on the metadata-load path (ATTACH / RESTORE), so a stored `RECOMPRESS CODEC(SZ3)` still loads and
# would only fail later, at the first TTL recompression merge, with `Codec SZ3 is lossy ...`. Check that such
# a recompression codec is normalized to the default codec on load, so the table stays writable and merges.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04612_pco_codec_recompress_ttl_attach"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/local"

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/metadata/local.sql"

# Lossy recompression codec: `get(ast, {})` throws while building it without a column type, so the ATTACH
# would fail during metadata load unless the codec is classified without going through that throwing path.
cat > "${WORKING_FOLDER}/metadata/local/t_sz3.sql" <<EOF
ATTACH TABLE local.t_sz3 (dt DateTime, s String) ENGINE=MergeTree ORDER BY tuple() TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(SZ3) SETTINGS min_bytes_for_wide_part=0;
EOF

# Experimental recompression codec that does not require a column type and is not lossy: it would not throw
# while resolving, but recompressing untyped part data with it is still invalid, so it must be reset too.
cat > "${WORKING_FOLDER}/metadata/local/t_alp.sql" <<EOF
ATTACH TABLE local.t_alp (dt DateTime, s String) ENGINE=MergeTree ORDER BY tuple() TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(ALP) SETTINGS min_bytes_for_wide_part=0;
EOF

# A chain whose first codec is unsafe must be reset as a whole.
cat > "${WORKING_FOLDER}/metadata/local/t_chain.sql" <<EOF
ATTACH TABLE local.t_chain (dt DateTime, s String) ENGINE=MergeTree ORDER BY tuple() TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(SZ3, LZ4) SETTINGS min_bytes_for_wide_part=0;
EOF

# Each table loads through the ATTACH path (codec checks relaxed). The rows are inserted with an already
# expired TTL, so `OPTIMIZE ... FINAL` performs the recompression merge, which resolves the recompression
# codec without a column type. After the unsafe codec is reset to the default codec on load, the merge must
# succeed and the data stays intact, instead of throwing at the codec resolution.
#
# The normalization is also durable: it rewrites the stored TTL AST, not only the parsed runtime codec, so a
# later unrelated `ALTER` (here `ADD COLUMN`, with `allow_suspicious_ttl_expressions = 0`) rebuilds the table
# TTL from that AST without hitting the now-removed unsafe codec again.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
INSERT INTO local.t_sz3 SELECT now() - INTERVAL 1 DAY, repeat('a', 100) FROM numbers(1000);
OPTIMIZE TABLE local.t_sz3 FINAL;
SELECT 'sz3', count(), sum(length(s)) FROM local.t_sz3;
ALTER TABLE local.t_sz3 ADD COLUMN extra UInt8 DEFAULT 0;
SELECT 'sz3_after_alter', count(), sum(length(s)) FROM local.t_sz3;
INSERT INTO local.t_alp SELECT now() - INTERVAL 1 DAY, repeat('b', 100) FROM numbers(1000);
OPTIMIZE TABLE local.t_alp FINAL;
SELECT 'alp', count(), sum(length(s)) FROM local.t_alp;
ALTER TABLE local.t_alp ADD COLUMN extra UInt8 DEFAULT 0;
SELECT 'alp_after_alter', count(), sum(length(s)) FROM local.t_alp;
INSERT INTO local.t_chain SELECT now() - INTERVAL 1 DAY, repeat('c', 100) FROM numbers(1000);
OPTIMIZE TABLE local.t_chain FINAL;
SELECT 'chain', count(), sum(length(s)) FROM local.t_chain;
"

rm -rf "${WORKING_FOLDER}"
