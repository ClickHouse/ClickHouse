#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
#
# Regression test for the actual pruning *effectiveness* of `spatial_bbox`, not just its
# correctness: on a realistic spatially-sorted dataset (data ordered by a geohash prefix,
# the common real-world layout for geo tables, so each granule's bbox is spatially tight),
# a small-area `pointInPolygon` predicate should skip the large majority of granules. This
# guards against a regression where the index still functions correctly but degrades to
# near-zero pruning effectiveness (e.g. an overly loose bbox union), which `04069`/`04853`'s
# correctness-only checks would not catch. Deterministic and fast: `rand(number)` is a fixed
# seed, so the granule counts below are exactly reproducible.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP TABLE IF EXISTS test_spatial_bbox_pruning_ratio;

CREATE TABLE test_spatial_bbox_pruning_ratio
(
    id   UInt64,
    geom Point,
    gh   String,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY gh
SETTINGS index_granularity = 64;

INSERT INTO test_spatial_bbox_pruning_ratio
SELECT
    number AS id,
    (rand(number) / 4294967295. * 360 - 180, rand(number + 1) / 4294967295. * 180 - 90) AS geom,
    geohashEncode(geom.1, geom.2) AS gh
FROM numbers(200000);

OPTIMIZE TABLE test_spatial_bbox_pruning_ratio FINAL;

-- Rectangle covering 1% of the full [-180,180] x [-90,90] extent: a realistic small-area
-- spatial query. On spatially-sorted data, the vast majority of granules cannot intersect it.
--
-- EXPLAIN's rows are joined into one string and matched with a single non-greedy, dot-matches-
-- newline ("(?s)") regex spanning from the "Name: idx_bbox" line to its own "Granules: X/Y"
-- line, rather than filtering rows independently: a plain "Granules: " substring match would
-- also catch the unrelated PrimaryKey section's own "Granules: 3125/3125" (no pruning there,
-- since ORDER BY is a geohash string, not the geometry itself). Asserting a ratio bound rather
-- than an exact granule count keeps this robust to minor, environment-dependent shifts in how
-- many spatially-adjacent points a granule boundary happens to split.
SELECT pruned_granules < total_granules * 0.1 FROM (
    SELECT
        CAST(splitByChar('/', ratio)[1], 'UInt64') AS pruned_granules,
        CAST(splitByChar('/', ratio)[2], 'UInt64') AS total_granules
    FROM (
        SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
        FROM (
            SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
            FROM (
                EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_pruning_ratio
                WHERE pointInPolygon(geom, [(-18., -9.), (18., -9.), (18., 9.), (-18., 9.), (-18., -9.)])
            )
        )
    )
);

DROP TABLE test_spatial_bbox_pruning_ratio;
EOF
