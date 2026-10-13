-- `-0.0 = 0.0`, but a bucketed `Map` puts a key in the bucket of the hash of its bits, which differ
-- for the two zeros. `m[k]` with a zero in `k` must also find the other zero, which may be in another bucket.

DROP TABLE IF EXISTS t_map_buckets_signed_zero;
CREATE TABLE t_map_buckets_signed_zero
(
    id UInt64,
    m Map(Float64, String),
    f Map(Float32, String),
    t Map(Tuple(Float64, UInt8), String),
    a Map(Array(Float64), String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0, max_buckets_in_map = 4;

INSERT INTO t_map_buckets_signed_zero VALUES
    (1, map(-0.0, 'a'), map(-0.0, 'a'), map((-0.0, 1), 'a'), map([-0.0], 'a')),
    (2, map(0.5, 'b'), map(0.5, 'b'), map((0.5, 1), 'b'), map([0.5], 'b')),
    (3, map(0.0, 'c'), map(0.0, 'c'), map((0.0, 1), 'c'), map([0.0], 'c')),
    (4, map(-0.0, 'd', 0.0, 'e'), map(0.0, 'd', -0.0, 'e'), map((0.0, 1), 'd', (-0.0, 1), 'e'), map([-0.0], 'd', [0.0], 'e'));

SET optimize_functions_to_subcolumns = 1;

SELECT id, m[0.0], m[-0.0], m[0.5], f[0.0], f[-0.0], t[(0.0, 1)], t[(-0.0, 1)], a[[0.0]], a[[-0.0]] FROM t_map_buckets_signed_zero ORDER BY id;
SELECT 'm[0.0] != \'\'', arraySort(groupArray(id)) FROM t_map_buckets_signed_zero WHERE m[0.0] != '';

DROP TABLE t_map_buckets_signed_zero;
