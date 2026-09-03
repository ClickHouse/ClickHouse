DROP TABLE IF EXISTS t_mutations_nondeterministic;

SET mutations_execute_subqueries_on_initiator = 1;
SET mutations_execute_nondeterministic_on_initiator = 1;

CREATE TABLE t_mutations_nondeterministic (`id` UInt64, `v` UInt64)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO t_mutations_nondeterministic VALUES (10, 20);

ALTER TABLE t_mutations_nondeterministic
    (UPDATE v = exists((
        SELECT count()
        FROM system.numbers
        WHERE number < 25
    )) WHERE 1);

ALTER TABLE t_mutations_nondeterministic (UPDATE v = now((exists((SELECT 1023)))) WHERE NULL);


DROP TABLE t_mutations_nondeterministic;
