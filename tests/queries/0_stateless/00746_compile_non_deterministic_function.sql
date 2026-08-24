SET compile_expressions = 1;
SET min_count_to_compile_expression = 1;

DROP TABLE IF EXISTS time_table;

CREATE TABLE time_table(timecol DateTime, value Int32) ENGINE = MergeTree order by tuple();

INSERT INTO time_table VALUES (now() - 5, 5), (now() - 3, 3);

SELECT COUNT() from time_table WHERE value < now() - 1 AND value != 0 AND modulo(value, 2) != 0 AND timecol < now() - 1;

SELECT sleep(3);

INSERT INTO time_table VALUES (now(), 101);

SELECT sleep(3);

SELECT COUNT() from time_table WHERE value < now() - 1 AND value != 0 AND modulo(value, 2) != 0 AND timecol < now() - 1;

DROP TABLE IF EXISTS time_table;
