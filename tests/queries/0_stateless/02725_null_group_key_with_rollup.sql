set allow_suspicious_low_cardinality_types=1;
DROP TABLE IF EXISTS group_by_null_key;
CREATE TABLE group_by_null_key (c1 Nullable(Int32), c2 LowCardinality(Nullable(Int32))) ENGINE = Memory();
INSERT INTO group_by_null_key VALUES (null, null), (null, null);

select c1, count(*) from group_by_null_key group by c1 WITH TOTALS;
select c2, count(*) from group_by_null_key group by c2 WITH TOTALS;

select c1, count(*) from group_by_null_key group by ROLLUP(c1);
select c2, count(*) from group_by_null_key group by ROLLUP(c2);


DROP TABLE group_by_null_key;
