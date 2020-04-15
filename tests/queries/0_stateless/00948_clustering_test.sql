DROP TABLE IF EXISTS defaults;
CREATE TABLE IF NOT EXISTS defaults
(
    param1 Float64,
    param2 Float64
) ENGINE = Memory;
insert into defaults values (0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0)

DROP TABLE IF EXISTS model;
CREATE TABLE model ENGINE = Memory AS SELECT incrementalClusteringState(4)(param1, param2) AS state FROM defaults;

DROP TABLE IF EXISTS answer;
create table answer engine = Memory as
select rowNumberInAllBlocks() as row_number, ans from
(with (select state from remote('127.0.0.1', currentDatabase(), model)) as model select evalMLMethod(model, param1, param2) as ans from remote('127.0.0.1', currentDatabase(), defaults));

select row_number from answer where ans = (select ans from answer where row_number = 0);
select row_number from answer where ans = (select ans from answer where row_number = 1);
select row_number from answer where ans = (select ans from answer where row_number = 2);
select row_number from answer where ans = (select ans from answer where row_number = 3);

DROP TABLE answer;
DROP TABLE model;
DROP TABLE defaults;
