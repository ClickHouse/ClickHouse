-- Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
-- This test asserts that column DEFAULT values from a table's schema are applied when
-- INSERTing `VALUES (NULL)` through `remote(...)` and `file(...)` table functions. With
-- `send_table_structure_on_insert_with_inline_data=0` the inline-server-parsed path does not
-- propagate the calling-side column descriptions to the receiving INSERT, so DEFAULTs are not
-- applied and NULLs become zero (e.g. `7` -> `0` for `c Int DEFAULT 7`, and `4 77` -> `4 0`
-- for the file case). Pin the legacy path; this is a separate column-description propagation
-- delta in the inline path and should be tracked independently.

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
