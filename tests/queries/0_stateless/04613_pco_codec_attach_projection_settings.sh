#!/usr/bin/env bash
# Tags: no-fasttest
# Projection-local `WITH SETTINGS` go through `ProjectionDescription::getProjectionFromAST`, which
# skips the projection settings allow-list and `MergeTreeSettings::sanityCheck` on the metadata-load
# path (ATTACH / SECONDARY_CREATE / RESTORE). So a projection whose stored metadata carries an untyped
# compression-codec setting (`marks_compression_codec` / `primary_key_compression_codec` /
# `default_compression_codec`) with a codec that requires a column type (the experimental PCO) or is
# lossy (SZ3) still loads, and would only fail later, when the first projection materialization or
# merge re-resolves the stored codec string without a column type. Check that the setting is
# normalized to the default codec on load, so inserts (which materialize the projection) and merges
# stay possible.

# The `Ordinary` database engine used for the offline metadata emits a warning; do not let it fail the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04613_pco_codec_attach_projection_settings"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/local"

echo "ATTACH DATABASE local ENGINE=Ordinary" > "${WORKING_FOLDER}/metadata/local.sql"

# `marks_compression_codec` compresses the projection part's marks directly (always untyped).
cat > "${WORKING_FOLDER}/metadata/local/t_marks.sql" <<EOF
ATTACH TABLE local.t_marks (x UInt32, y UInt32, PROJECTION p (SELECT y, x ORDER BY y) WITH SETTINGS (marks_compression_codec = 'PCO')) ENGINE=MergeTree ORDER BY x;
EOF

# `primary_key_compression_codec` compresses the projection part's primary key directly (always untyped).
cat > "${WORKING_FOLDER}/metadata/local/t_pk.sql" <<EOF
ATTACH TABLE local.t_pk (id UInt64, v UInt64, PROJECTION p (SELECT v, id ORDER BY v) WITH SETTINGS (primary_key_compression_codec = 'PCO')) ENGINE=MergeTree ORDER BY id;
EOF

# The same must hold for the lossy SZ3 codec: resolving it without a column type throws while building
# the codec, so the sanitization must classify it without going through that throwing path.
cat > "${WORKING_FOLDER}/metadata/local/t_marks_sz3.sql" <<EOF
ATTACH TABLE local.t_marks_sz3 (x UInt32, y UInt32, PROJECTION p (SELECT y, x ORDER BY y) WITH SETTINGS (marks_compression_codec = 'SZ3')) ENGINE=MergeTree ORDER BY x;
EOF

# A safe projection setting in the same clause must survive the sanitization untouched.
cat > "${WORKING_FOLDER}/metadata/local/t_mixed.sql" <<EOF
ATTACH TABLE local.t_mixed (x UInt32, y UInt32, PROJECTION p (SELECT y, x ORDER BY y) WITH SETTINGS (index_granularity = 4096, marks_compression_codec = 'PCO')) ENGINE=MergeTree ORDER BY x;
EOF

# Each table loads through the ATTACH path (allow-list and sanity checks skipped). The INSERT
# materializes the projection part, so without the load-path normalization it would already throw
# (the PCO codec cannot be created without a numeric column type); OPTIMIZE FINAL then rewrites the
# projection through the merge path. The normalization must also be durable: the stored projection
# definition AST is re-parsed by ALTERs that recalculate projections, so an unrelated ALTER ADD COLUMN
# after the attach must succeed as well. system.projections.settings shows the effective projection
# settings: the unsafe codec settings must be gone, the safe ones kept.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --multiquery "
INSERT INTO local.t_marks SELECT number, number * 2 FROM numbers(1000);
SELECT 'marks', count() FROM local.t_marks;
OPTIMIZE TABLE local.t_marks FINAL;
ALTER TABLE local.t_marks ADD COLUMN z UInt32;
SELECT 'marks_after_alter', count() FROM local.t_marks;
SELECT 'marks_settings', settings FROM system.projections WHERE database = 'local' AND table = 't_marks';
INSERT INTO local.t_pk SELECT number, number * 2 FROM numbers(1000);
SELECT 'primary_key', count() FROM local.t_pk;
OPTIMIZE TABLE local.t_pk FINAL;
ALTER TABLE local.t_pk ADD COLUMN w UInt64;
SELECT 'primary_key_after_alter', count() FROM local.t_pk;
INSERT INTO local.t_marks_sz3 SELECT number, number * 2 FROM numbers(1000);
SELECT 'marks_sz3', count() FROM local.t_marks_sz3;
INSERT INTO local.t_mixed SELECT number, number * 2 FROM numbers(1000);
SELECT 'mixed', count() FROM local.t_mixed;
SELECT 'mixed_settings', settings FROM system.projections WHERE database = 'local' AND table = 't_mixed';
"

rm -rf "${WORKING_FOLDER}"
