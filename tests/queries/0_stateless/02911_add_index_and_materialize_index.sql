-- Tags: no-replicated-database

DROP TABLE IF EXISTS index_test;

CREATE TABLE index_test
(
	x UInt32,
	y UInt32,
	z UInt32
) ENGINE = MergeTree order by x;

ALTER TABLE index_test
    ADD INDEX i_x mortonDecode(2, z).1 TYPE minmax GRANULARITY 1,
    ADD INDEX i_y mortonDecode(2, z).2 TYPE minmax GRANULARITY 1,
    MATERIALIZE INDEX i_x,
    MATERIALIZE INDEX i_y;

drop table index_test;
