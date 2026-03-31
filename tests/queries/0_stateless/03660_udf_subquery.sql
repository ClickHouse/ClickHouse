-- Tags: no-parallel

SET enable_analyzer = 1;

DROP TABLE IF EXISTS dict;
create table dict engine=MergeTree() order by id as
select 1 as id, 'one' as name union all
select 2 as id, 'two' as name;

CREATE OR REPLACE FUNCTION udf_type_of_int AS
int_ -> (select if(name = 'one', 'The One', 'other') from dict where id = int_);

-- this part worked successfully
SELECT * FROM (
select udf_type_of_int(1) union all
select udf_type_of_int(2)
) ORDER BY ALL;

SELECT '';

-- ... and this not!
select udf_type_of_int(number) from numbers(5) order by number;

SELECT '';

select number as id, udf_type_of_int(id) from numbers(5) order by number;

SELECT '';

select number as id, udf_type_of_int(id or id = 1) from numbers(5) order by number;

DROP FUNCTION udf_type_of_int;
DROP TABLE dict;
