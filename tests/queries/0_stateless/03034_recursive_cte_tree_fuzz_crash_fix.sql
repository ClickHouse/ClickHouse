SET enable_analyzer = 1;
SET enable_global_with_statement=1;
SET session_timezone = 'Etc/UTC';

DROP TABLE IF EXISTS department__fuzz_1;
CREATE TABLE department__fuzz_1 (`id` DateTime, `parent_department` UInt128, `name` String) ENGINE = TinyLog;

INSERT INTO department__fuzz_1 VALUES (0, NULL, 'ROOT');
INSERT INTO department__fuzz_1 VALUES (1, 0, 'A');
INSERT INTO department__fuzz_1 VALUES (2, 1, 'B');
INSERT INTO department__fuzz_1 VALUES (3, 2, 'C');
INSERT INTO department__fuzz_1 VALUES (4, 2, 'D');
INSERT INTO department__fuzz_1 VALUES (5, 0, 'E');
INSERT INTO department__fuzz_1 VALUES (6, 4, 'F');
INSERT INTO department__fuzz_1 VALUES (7, 5, 'G');

DROP TABLE IF EXISTS department__fuzz_3;
CREATE TABLE department__fuzz_3 (`id` Date, `parent_department` UInt128, `name` LowCardinality(String)) ENGINE = TinyLog;

INSERT INTO department__fuzz_3 VALUES (0, NULL, 'ROOT');
INSERT INTO department__fuzz_3 VALUES (1, 0, 'A');
INSERT INTO department__fuzz_3 VALUES (2, 1, 'B');
INSERT INTO department__fuzz_3 VALUES (3, 2, 'C');
INSERT INTO department__fuzz_3 VALUES (4, 2, 'D');
INSERT INTO department__fuzz_3 VALUES (5, 0, 'E');
INSERT INTO department__fuzz_3 VALUES (6, 4, 'F');
INSERT INTO department__fuzz_3 VALUES (7, 5, 'G');

SELECT * FROM
(
    WITH RECURSIVE q AS
    (
        SELECT * FROM department__fuzz_3
        UNION ALL
        (
            WITH RECURSIVE x AS
            (
                SELECT * FROM department__fuzz_1
                UNION ALL
                (SELECT * FROM q UNION ALL SELECT * FROM x)
            )
            SELECT * FROM x
        )
    )
    SELECT * FROM q LIMIT 32
)
ORDER BY id ASC, parent_department DESC, name ASC;

DROP TABLE department__fuzz_1;
DROP TABLE department__fuzz_3;
