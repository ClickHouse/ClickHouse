drop table if exists order_by_nulls_first;

CREATE TABLE  order_by_nulls_first
(diff Nullable(Int16), traf UInt64)
ENGINE = MergeTree ORDER BY tuple();

insert into order_by_nulls_first values (NULL,1),(NULL,0),(NULL,0),(NULL,0),(NULL,0),(NULL,0),(28,0),(0,0);

SELECT
    diff,
    traf
FROM order_by_nulls_first
order by diff desc NULLS FIRST, traf
limit 1, 4;

select '---';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff DESC NULLS FIRST,
    traf ASC;

drop table if exists order_by_nulls_first;