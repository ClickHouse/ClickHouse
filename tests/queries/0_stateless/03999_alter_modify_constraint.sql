DROP TABLE IF EXISTS t_modify_constraint;

CREATE TABLE t_modify_constraint
(
    a UInt32,
    b UInt32,
    CONSTRAINT c_a CHECK a < 10,
    CONSTRAINT c_b CHECK b < 10
)
ENGINE = MergeTree ORDER BY a;

-- Helper that prints constraints in declaration order (robust to db-name and engine-settings randomization).
SELECT 'initial';
SELECT g[1], g[2], g[3] FROM
(
    SELECT arrayJoin(extractAllGroupsVertical(create_table_query, 'CONSTRAINT (\\w+) (CHECK|ASSUME) ([^,)]+)')) AS g
    FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_constraint'
);

-- Original constraint forbids a >= 10.
INSERT INTO t_modify_constraint VALUES (15, 1); -- { serverError VIOLATED_CONSTRAINT }

ALTER TABLE t_modify_constraint MODIFY CONSTRAINT c_a CHECK a < 100;

-- c_a keeps its position (still before c_b) and only its expression changed.
SELECT 'after modify c_a';
SELECT g[1], g[2], g[3] FROM
(
    SELECT arrayJoin(extractAllGroupsVertical(create_table_query, 'CONSTRAINT (\\w+) (CHECK|ASSUME) ([^,)]+)')) AS g
    FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_constraint'
);

-- The relaxed constraint now allows a = 15 but still forbids a >= 100.
INSERT INTO t_modify_constraint VALUES (15, 1);
INSERT INTO t_modify_constraint VALUES (150, 1); -- { serverError VIOLATED_CONSTRAINT }
SELECT * FROM t_modify_constraint ORDER BY a;

-- MODIFY can switch the constraint kind from CHECK to ASSUME as well.
ALTER TABLE t_modify_constraint MODIFY CONSTRAINT c_b ASSUME b < 1000;
SELECT 'after modify c_b';
SELECT g[1], g[2], g[3] FROM
(
    SELECT arrayJoin(extractAllGroupsVertical(create_table_query, 'CONSTRAINT (\\w+) (CHECK|ASSUME) ([^,)]+)')) AS g
    FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_constraint'
);

-- MODIFY of a missing constraint throws, unless IF EXISTS is given.
ALTER TABLE t_modify_constraint MODIFY CONSTRAINT c_missing CHECK a < 5; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_constraint MODIFY CONSTRAINT IF EXISTS c_missing CHECK a < 5;

SELECT 'final';
SELECT g[1], g[2], g[3] FROM
(
    SELECT arrayJoin(extractAllGroupsVertical(create_table_query, 'CONSTRAINT (\\w+) (CHECK|ASSUME) ([^,)]+)')) AS g
    FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_constraint'
);

DROP TABLE t_modify_constraint;
