DROP TABLE IF EXISTS 03562_t0, 03562_t1;

CREATE TABLE 03562_t0
(
    id UInt32 DEFAULT 0,
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE 03562_t1
(
    filter_id UInt32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO 03562_t0
SELECT
    number AS id
FROM numbers(1000000);


insert into 03562_t1 values(1);

-- At this point, the result returns 1
SELECT count()
FROM 03562_t0
WHERE id IN (
    SELECT filter_id
    FROM 03562_t1
);

insert into 03562_t1 values(100001);

-- At this point, the result should be returns 2
SELECT count()
FROM 03562_t0
WHERE id IN (
    SELECT filter_id
    FROM 03562_t1
);

DROP TABLE IF EXISTS 03562_t0, 03562_t1;
