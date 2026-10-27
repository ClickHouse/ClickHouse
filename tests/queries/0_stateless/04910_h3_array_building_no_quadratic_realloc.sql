-- Tags: no-fasttest

-- These two build the result array over many rows and carry the timing oracle.
-- max_block_size is pinned: the accumulator is per block, so the cost is linear in it.

SELECT sum(length(h3kRing(materialize(579205133326352383), materialize(toUInt16(1)))))
FROM numbers(150000)
SETTINGS max_block_size = 65409, max_execution_time = 15;

SELECT sum(length(h3ToChildren(materialize(599405990164561919), materialize(6))))
FROM numbers(150000)
SETTINGS max_block_size = 65409, max_execution_time = 15;

-- The two polygon functions share the same fixed line. Each sum below must be non-zero,
-- otherwise no cells were produced and the accumulator was never grown.

SELECT sum(length(h3PolygonToCells([(materialize(55.66824), 12.595493), (55.667901, 12.593991), (55.667474, 12.595117), (55.66824, 12.595493)], 11)))
FROM numbers(2000);

SELECT sum(length(h3PolygonToCellsWithContainment([(materialize(-122.4089866999972145), 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 9, 0)))
FROM numbers(200);

-- The results themselves must be unchanged.

SELECT arraySort(h3kRing(579205133326352383, toUInt16(1)));
SELECT h3kRing(579205133326352383, toUInt16(0));
SELECT length(h3kRing(materialize(579205133326352383), materialize(toUInt16(2))));
SELECT arraySort(h3ToChildren(599405990164561919, 6)) = arraySort(h3ToChildren(materialize(599405990164561919), materialize(6)));
SELECT length(h3PolygonToCells([(55.66824, 12.595493), (55.667901, 12.593991), (55.667474, 12.595117), (55.66824, 12.595493)], 11));

-- Rows that produce nothing must not shift the offsets of the rows around them.

SELECT length(h3kRing(arrayJoin([toUInt64(0), 579205133326352383, toUInt64(0)]), materialize(toUInt16(1))))
SETTINGS functions_h3_default_if_invalid = 1;
