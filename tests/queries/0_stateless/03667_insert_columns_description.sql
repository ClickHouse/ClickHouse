-- Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
-- This test pins `send_table_structure_on_insert_with_inline_data` to 1 (legacy path).
-- With setting = 0 (server-parsed inline path), the `DEFAULT`/`input_format_null_as_default`
-- value is not applied for `INSERT INTO TABLE FUNCTION ...`: an explicit `NULL` becomes 0
-- instead of the column default (a direct table `INSERT` is unaffected). See the tracking
-- issue https://github.com/ClickHouse/ClickHouse/issues/109253. Once it is fixed, the pin
-- can be removed so this test exercises both paths.
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
