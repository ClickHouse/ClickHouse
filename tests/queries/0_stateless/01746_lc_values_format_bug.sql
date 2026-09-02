drop table if exists lc_test;

CREATE TABLE lc_test
(
    `id` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY id;

insert into lc_test values (toString('a'));

select id from lc_test;
drop table if exists lc_test;
