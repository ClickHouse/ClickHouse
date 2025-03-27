--- Checks if the skip indices are correctly used depending on the filter predicates -- issue #75228.

DROP TABLE IF EXISTS `example`;
CREATE TABLE `example`
(
    `id` UInt32,
    `colA` String,
    `colB` String,
    INDEX `colA_idx` colA TYPE bloom_filter GRANULARITY 1,
    INDEX `colB_idx` colB TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY `id`
SETTINGS index_granularity = 1;

INSERT INTO `example` VALUES(0, 'aa', 'ba'),(1, 'ab', 'bb'),(2, 'ac', 'bc'),(3, 'ad', 'bd'),(4, 'ae', 'be');

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE id IN (0,1,2,3,4) AND colA = 'aa';

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE id IN (0,1,2,3,4) AND colB = 'bb';

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE id IN (0,1,2,3,4) AND (colA = 'aa' OR colB = 'bb'); 

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE id IN (0,1,2,3,4) AND NOT(colA != 'aa' AND colB != 'bb'); 

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE 1 = 1 OR colA = 'aa'; -- short circuit, don't need the skip index.

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE 1 = 2 AND colA = 'aa'; -- short circuit, don't need the skip index.

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE id IN (0,1,2,3,4) AND (colB = 'bb' OR 1 = 1); -- short circuit, don't need the skip index

EXPLAIN indexes = 1
SELECT count()
FROM `example`
WHERE id IN (0,1,2,3,4) AND (colB = 'bb' AND 1 = 2); -- short circuit, neither primary index nor skip indices needed

DROP TABLE `example`;
