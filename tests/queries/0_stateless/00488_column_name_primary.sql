DROP TABLE IF EXISTS primary;

CREATE TABLE primary
(
    `primary` String
)
ENGINE = MergeTree
ORDER BY primary
settings min_bytes_for_wide_part=0,min_bytes_for_wide_part=0
 AS
SELECT *
FROM numbers(1000);

select max(primary) from primary;

DROP TABLE primary;
