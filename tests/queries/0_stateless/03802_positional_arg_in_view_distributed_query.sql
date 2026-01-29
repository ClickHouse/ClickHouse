-- Clean up if needed
drop table if exists test_table SYNC;
drop view if exists test_view SYNC;

-- Create a simple base table
create table test_table ( str String ) ENGINE = MergeTree
order by str;

-- Insert some sample data (optional, but helps verify locally)
insert into test_table values ('a'), ('b'), ('c');

-- Create a view using positional GROUP BY
create view test_view as
select str
from test_table
group by str;


select str from test_table;

-- Simulate distributed query to "remote" nodes (points back to localhost or multiple addresses)
select str
from remote('127.0.0.{1|2|3}', currentDatabase(), test_view)
order by str;

-- Clean up
drop table if exists test_table SYNC;
drop view if exists test_view SYNC;
