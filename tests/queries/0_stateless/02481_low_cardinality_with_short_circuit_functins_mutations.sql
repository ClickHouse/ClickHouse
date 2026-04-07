drop table if exists issue_46128;

create table issue_46128 (
	id Int64,
	a LowCardinality(Nullable(String)),
	b LowCardinality(Nullable(String))
) Engine = MergeTree order by id
as  select number%100, 'xxxx', 'yyyy' from numbers(10);

ALTER TABLE issue_46128 UPDATE a = b WHERE id= 1 settings mutations_sync=2;

select * from issue_46128 where id  <= 2 order by id;

drop table issue_46128;
