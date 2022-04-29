drop table if exists data_02291;
create table data_02291 (parent_key Int, child_key Int, value Int) engine=MergeTree() order by parent_key;
insert into data_02291 select number%10, number%3, number from numbers(100);

-- { echoOn }
SELECT groupArraySorted([NULL, NULL, NULL, NULL])
FROM data_02291
GROUP BY parent_key
    WITH TOTALS
ORDER BY parent_key DESC
SETTINGS optimize_aggregation_in_order = 0;

SELECT groupArraySorted([NULL, NULL, NULL, NULL])
FROM data_02291
GROUP BY parent_key
    WITH TOTALS
ORDER BY parent_key DESC
SETTINGS optimize_aggregation_in_order = 1;
