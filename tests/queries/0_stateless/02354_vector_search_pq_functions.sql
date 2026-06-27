-- The trained Product Quantization SQL functions `pqTrain`, `pqEncode`, and `pqDistance` train sub-codebooks, encode
-- vectors into codes, and compute the asymmetric distance between a code and a full-precision query. They underpin the
-- `pq` method of the `Quantize(...)` codec and are usable directly.

-- pqTrain returns the flat codebook: 2^nbits centroids of `dimensions` floats. With nbits = 1 and dimensions = 4 that is
-- 2 * 4 * 4 = 32 bytes.
SELECT 'codebook_length', length(pqTrain([[1., 2., 3., 4.], [5., 6., 7., 8.]], 4, 2, 1));

-- pqEncode returns one m-byte code per vector for nbits <= 8, and 2m bytes for nbits in (8, 16].
SELECT 'code_length_8bit', length(pqEncode([1., 2., 3., 4.], pqTrain([[1., 2., 3., 4.]], 4, 2, 1), 4, 2, 1));
SELECT 'code_length_9bit', length(pqEncode(arrayMap(j -> toFloat32(j), range(4)), pqTrain([arrayMap(j -> toFloat32(j), range(4))], 4, 2, 9), 4, 2, 9));

-- A codebook trained on a single vector reconstructs it exactly, so its asymmetric distance to itself is zero.
WITH pqTrain([[1., 0.]], 2, 2, 1) AS cb
SELECT 'self_distance', round(pqDistance(pqEncode([1., 0.], cb, 2, 2, 1), cb, [1., 0.], 2, 2, 1, 1), 3);

-- Ranking: with two well-separated clusters, the code of the cluster nearer to the query has the smaller distance.
WITH pqTrain([[0., 0., 0., 0.], [10., 10., 10., 10.]], 4, 2, 1) AS cb,
     pqEncode([0., 0., 0., 0.], cb, 4, 2, 1) AS code_near,
     pqEncode([10., 10., 10., 10.], cb, 4, 2, 1) AS code_far
SELECT 'ranking_l2',
    pqDistance(code_near, cb, [0.5, 0.5, 0.5, 0.5], 4, 2, 1, 1) < pqDistance(code_far, cb, [0.5, 0.5, 0.5, 0.5], 4, 2, 1, 1);

-- The codes and codebook round-trip through a column (the codebook applies uniformly across rows).
DROP TABLE IF EXISTS pq_fn;
CREATE TABLE pq_fn (id UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO pq_fn SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(16)) FROM numbers(500);
WITH (SELECT pqTrain(groupArray(vec), 16, 4, 4) FROM pq_fn) AS cb
SELECT 'column_self_is_nearest',
    (SELECT id FROM pq_fn ORDER BY pqDistance(pqEncode(vec, cb, 16, 4, 4), cb, (SELECT vec FROM pq_fn WHERE id = 42), 16, 4, 4, 1) ASC, id LIMIT 1) = 42;
DROP TABLE pq_fn;

-- pqTrain validates its parameters (dimensions must be a multiple of m; nbits in [1, 16]).
SELECT pqTrain([[1., 2., 3., 4.]], 4, 3, 1); -- { serverError BAD_ARGUMENTS }
SELECT pqTrain([[1., 2., 3., 4.]], 4, 2, 17); -- { serverError BAD_ARGUMENTS }
