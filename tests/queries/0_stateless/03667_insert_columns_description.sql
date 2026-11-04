SELECT '-- remote table function columns description';
CREATE TABLE t0 (c Int DEFAULT 7) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE FUNCTION remote('localhost:9000', database(), 't0', 'default', '') VALUES (NULL);
INSERT INTO TABLE t0 VALUES (NULL);

SELECT * FROM t0 ORDER BY ALL;

CREATE TABLE fuzz_87972 (c0 Int MATERIALIZED 1, c1 Int EPHEMERAL) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE FUNCTION remote('localhost:9000', database(), 'fuzz_87972', 'default', '') VALUES (); -- { error EMPTY_LIST_OF_COLUMNS_PASSED }

SELECT '-- file table function columns description';
INSERT INTO TABLE FUNCTION file(database() || '_test.csv', CSV, 'a Int, b Int DEFAULT 77') SELECT number, if(number%2=1, NULL, number) FROM numbers(3);
INSERT INTO TABLE FUNCTION file(database() || '_test.csv', CSV, 'a Int, b Int DEFAULT 77') VALUES (3, 3), (4, NULL);

SELECT * FROM file(database() || '_test.csv') ORDER BY ALL;
