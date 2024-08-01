SET allow_experimental_analyzer = 1;

SELECT concat(materialize(toLowCardinality('b')), 'a') FROM remote('127.0.0.{1,2}', system, one) GROUP BY 'a';

SELECT concat(NULLIF(1, materialize(toLowCardinality(1))), concat(NULLIF(1, 1))) FROM remote('127.0.0.{1,2}', system, one) GROUP BY concat(NULLIF(1, 1));

DROP TABLE IF EXISTS test__fuzz_21;
CREATE TABLE test__fuzz_21
(
    `x` Decimal(18, 10)
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO test__fuzz_21 VALUES (1), (2), (3);

WITH (
        SELECT CAST(toFixedString(toFixedString(materialize(toFixedString('111111111111111111111111111111111111111', 39)), 39), 39), 'UInt128')
    ) AS v
SELECT
    coalesce(materialize(toLowCardinality(toNullable(1))), 10, NULL),
    max(v)
FROM remote('127.0.0.{1,2}', currentDatabase(), test__fuzz_21)
GROUP BY
    coalesce(NULL),
    coalesce(1, 10, 10, materialize(NULL));
