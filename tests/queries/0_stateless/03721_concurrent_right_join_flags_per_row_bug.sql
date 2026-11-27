SET join_algorithm = 'parallel_hash';
SET enable_analyzer = 1;

SET join_use_nulls = 1;

SELECT ty.number, sipHash64(ty.number + 1) % 100 as a, tw.number, sipHash64(tw.number) % 100 as b
FROM numbers(1, 4) ty
RIGHT JOIN numbers(1, 4) tw
ON tw.number = ty.number
     AND a <= b
ORDER BY ALL
;
