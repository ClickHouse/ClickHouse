-- https://github.com/ClickHouse/ClickHouse/issues/4567
SET enable_analyzer=1;
DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS animals;
DROP TABLE IF EXISTS colors;

create table fact(id Int64, animal_key Int64, color_key Int64) Engine = MergeTree order by tuple();
insert into fact values (1,1,1),(2,2,2);

create table animals(animal_key UInt64, animal_name String) Engine = MergeTree order by tuple();
insert into animals values (0, 'unknown');

create table colors(color_key UInt64, color_name String) Engine = MergeTree order by tuple();
insert into colors values (0, 'unknown');


select  id, animal_name, a.animal_key, color_name, color_key
from fact a
        left join (select toInt64(animal_key) animal_key, animal_name from animals) b on (a.animal_key = b.animal_key)
        left join (select toInt64(color_key) color_key, color_name from colors) c on (a.color_key = c.color_key);  -- { serverError AMBIGUOUS_IDENTIFIER }

select  id, animal_name, animal_key, color_name, color_key
from fact a
        left join (select toInt64(animal_key) animal_key, animal_name from animals) b on (a.animal_key = b.animal_key)
        left join (select toInt64(color_key) color_key, color_name from colors) c on (a.color_key = c.color_key);  -- { serverError AMBIGUOUS_IDENTIFIER }
