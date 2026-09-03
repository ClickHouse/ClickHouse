drop table if exists tab;
create table tab (a LowCardinality(String), b LowCardinality(String)) engine = MergeTree partition by a order by tuple() settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

insert into tab values ('1', 'a'), ('2', 'b');
SELECT a = '1' FROM tab WHERE a = '1' and b='a';

-- Fuzzed
SELECT * FROM tab WHERE (a = '1') AND 0 AND (b = 'a');

drop table if exists tab;
