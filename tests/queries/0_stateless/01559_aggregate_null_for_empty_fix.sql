SELECT MAX(aggr)
FROM
(
	SELECT MAX(-1) AS aggr
	FROM system.one
	WHERE NOT 1
	UNION ALL
	SELECT MAX(-1) AS aggr
	FROM system.one
	WHERE 1

);
SELECT MaX(aggr)
FROM
(
    SELECT mAX(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT MAx(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT MaX(aggr)
FROM
(
    SELECT mAX(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT MaX(aggr)
FROM
(
    SELECT mAX(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE not 1
);
SET aggregate_functions_null_for_empty=1;
SELECT MAX(aggr)
FROM
(
	SELECT MAX(-1) AS aggr
	FROM system.one
	WHERE NOT 1
	UNION ALL
	SELECT MAX(-1) AS aggr
	FROM system.one
	WHERE 1

);
SELECT MaX(aggr)
FROM
(
    SELECT mAX(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT MAx(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT MaX(aggr)
FROM
(
    SELECT mAX(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT MaX(aggr)
FROM
(
    SELECT mAX(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE not 1
);
