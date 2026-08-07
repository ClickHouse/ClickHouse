drop table if exists tbl_repr;

CREATE TABLE tbl_repr(
ts DateTime,
x  String)
ENGINE=MergeTree ORDER BY ts;


SELECT *
FROM
(
    SELECT
        x,
        length(x)
    FROM tbl_repr
    WHERE ts > now()
    LIMIT 1
)
WHERE x != '';

drop table if exists tbl_repr;
