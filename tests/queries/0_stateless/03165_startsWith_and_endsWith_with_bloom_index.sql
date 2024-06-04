set allow_experimental_inverted_index=1;

    DROP TABLE IF EXISTS example_table;

CREATE TABLE example_table
(
    `id` UInt32,
    `name` String,
    `addr` String,
    INDEX idx_name name TYPE tokenbf_v1(1024, 3, 0) GRANULARITY 1,
    INDEX idx_addr addr TYPE full_text GRANULARITY 1
)
    ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO example_table (id, name, addr) VALUES (1, 'Alice Lennon', 'Heron Way'), (2, 'Bob','Wall'), (3, 'Michael Jorden', '3rd Street Northeast');

SELECT * FROM example_table WHERE startsWith(name, 'Whoever');
SELECT * FROM example_table WHERE startsWith(name, 'Bob');
SELECT * FROM example_table WHERE startsWith(name, 'Alice');
SELECT * FROM example_table WHERE startsWith(name, 'Michael Jor');
SELECT * FROM example_table WHERE startsWith(name, 'Michael Jorden');

SELECT * FROM example_table WHERE endsWith(addr, 'Whatever');
SELECT * FROM example_table WHERE endsWith(addr, 'Wall');
SELECT * FROM example_table WHERE endsWith(addr, 'Way');
SELECT * FROM example_table WHERE endsWith(addr, 'eet Northeast');
SELECT * FROM example_table WHERE endsWith(addr, 'Street Northeast');
