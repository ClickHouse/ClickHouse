SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]);
SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2, 1]); -- { serverError ILLEGAL_COLUMN }

SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.75, 0.65, 0.6, 0.78, 0.7, 0.05, 0.4, 0.4, 0.05, 0.5, 0.1, 0.1], [1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

SELECT arrayNormalizedGini(arrayResize([1], 2000000), arrayResize([1], 2000000)); -- { serverError TOO_LARGE_ARRAY_SIZE }

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    `a1` Array(Float32),
    `a2` Array(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t VALUES ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]), ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]), ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]), ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]);

SELECT arrayNormalizedGini(a1, a2) FROM t;

SELECT arrayNormalizedGini(a1, [6, 1, 0, 2]) FROM t;
SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7], a2) FROM t;

-- Array lengths only need to match within each row, not across rows in the same block.
SELECT tupleElement(arrayNormalizedGini(range(number + 2), range(number + 2)), 3)
FROM numbers(2)
SETTINGS max_block_size = 2;

-- The result must not depend on the block size.
SELECT tupleElement(arrayNormalizedGini(range(number + 2), range(number + 2)), 3)
FROM numbers(2)
SETTINGS max_block_size = 1;

-- The complete later row must affect the result.
SELECT round(
    tupleElement(
        arrayNormalizedGini(
            if(number = 0, [0., 1.], [0., 1., 2.]),
            if(number = 0, [0., 1.], [1., 0., 2.])
        ),
        3
    ),
    6
)
FROM numbers(2)
SETTINGS max_block_size = 2;

-- Mismatched arrays must still be rejected when the mismatch appears in a later row.
SELECT arrayNormalizedGini(
    range(if(number = 0, 1, 2)),
    range(if(number = 0, 1, 3)))
FROM numbers(2)
SETTINGS max_block_size = 2; -- { serverError ILLEGAL_COLUMN }

-- Grouped arrays are a realistic source of different per-row array lengths.
SELECT tupleElement(arrayNormalizedGini(predictions, labels), 3)
FROM
(
    SELECT
        group_id,
        groupArray(toFloat64(number)) AS predictions,
        groupArray(toFloat64(number)) AS labels
    FROM
    (
        SELECT number, if(number < 2, 0, 1) AS group_id
        FROM numbers(5)
    )
    GROUP BY group_id
    ORDER BY group_id
)
SETTINGS max_block_size = 2;

-- The size limit must also be checked for a later row.
SELECT arrayNormalizedGini(
    range(if(number = 0, 1, 1048577)),
    range(if(number = 0, 1, 1048577)))
FROM numbers(2)
SETTINGS max_block_size = 2; -- { serverError TOO_LARGE_ARRAY_SIZE }

DROP TABLE t;
