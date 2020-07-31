DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `Source.C1` Array(UInt64),
    `Source.C2` Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY tuple();

SET optimize_move_functions_out_of_any = 1;

SELECT any(arrayFilter((c, d) -> (4 = d), `Source.C1`, `Source.C2`)[1]) AS x
FROM test
WHERE 0
GROUP BY 42;

DROP TABLE test;
