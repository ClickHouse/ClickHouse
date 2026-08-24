DROP TABLE IF EXISTS mutation_table;
CREATE TABLE mutation_table (
    id int,
    price Nullable(Int32)
)
ENGINE = MergeTree()
PARTITION BY id
ORDER BY id;

INSERT INTO mutation_table (id, price) VALUES (1, 100);

ALTER TABLE mutation_table UPDATE price = 150 WHERE id = 1 SETTINGS mutations_sync = 2;

SELECT * FROM mutation_table;

DROP TABLE IF EXISTS mutation_table;



create table mutation_table (  dt Nullable(Date), name Nullable(String))
engine MergeTree order by tuple();

insert into mutation_table (name, dt) values ('car', '2020-02-28');
insert into mutation_table (name, dt) values ('dog', '2020-03-28');

select * from mutation_table order by dt, name;

alter table mutation_table update dt = toDateOrNull('2020-08-02')
where name = 'car' SETTINGS mutations_sync = 2;

select * from mutation_table order by dt, name;

insert into mutation_table (name, dt) values ('car', Null);
insert into mutation_table (name, dt) values ('cat', Null);

alter table mutation_table update dt = toDateOrNull('2020-08-03')
where name = 'car' and dt is null SETTINGS mutations_sync = 2;

select * from mutation_table order by dt, name;

alter table mutation_table update dt = toDateOrNull('2020-08-04')
where name = 'car' or dt is null SETTINGS mutations_sync = 2;

select * from mutation_table order by dt, name;

insert into mutation_table (name, dt) values (Null, '2020-08-05');

alter table mutation_table update dt = Null
where name is not null SETTINGS mutations_sync = 2;

select * from mutation_table order by dt, name;


DROP TABLE IF EXISTS mutation_table;
