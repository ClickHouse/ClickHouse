DROP TABLE IF EXISTS mutation_table;
SET enable_lightweight_update = 1;

CREATE TABLE mutation_table
(
    id int,
    price Nullable(Int32)
)
ENGINE = MergeTree()
PARTITION BY id
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO mutation_table (id, price) VALUES (1, 100);

UPDATE mutation_table SET price = 150 WHERE id = 1;

SELECT * FROM mutation_table;

DROP TABLE IF EXISTS mutation_table;

create table mutation_table (dt Nullable(Date), name Nullable(String))
engine MergeTree order by tuple()
settings enable_block_number_column = 1, enable_block_offset_column = 1;

insert into mutation_table (name, dt) values ('car', '2020-02-28');
insert into mutation_table (name, dt) values ('dog', '2020-03-28');

select * from mutation_table order by dt, name;

update mutation_table set dt = toDateOrNull('2020-08-02') where name = 'car';

select * from mutation_table order by dt, name;

insert into mutation_table (name, dt) values ('car', null);
insert into mutation_table (name, dt) values ('cat', null);

update mutation_table set dt = toDateOrNull('2020-08-03') where name = 'car' and dt is null;

select * from mutation_table order by dt, name;

update mutation_table set dt = toDateOrNull('2020-08-04') where name = 'car' or dt is null;

select * from mutation_table order by dt, name;

insert into mutation_table (name, dt) values (null, '2020-08-05');

update mutation_table set dt = null where name is not null;

select * from mutation_table order by dt, name;

DROP TABLE IF EXISTS mutation_table;
