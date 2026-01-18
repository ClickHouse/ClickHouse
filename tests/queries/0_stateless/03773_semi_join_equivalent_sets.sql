SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;

CREATE TABLE users (uid UInt64, name String, age Int16) ENGINE=MergeTree ORDER BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT * FROM users LEFT ANY JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE t2.number = 0
) WHERE explain ilike '%Type:%' OR explain ilike '%Strictness%' OR explain ilike '%filter column%';

EXPLAIN actions = 1
SELECT * FROM users LEFT SEMI JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE t2.number = 1;

SELECT explain FROM (
  EXPLAIN actions = 1 SELECT * FROM users LEFT ANY JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE t2.number = 1
) WHERE explain ilike '%Type:%' OR explain ilike '%Strictness%' OR explain ilike '%filter column%';

SELECT '--';

SELECT explain FROM (
  EXPLAIN actions = 1 SELECT * FROM users LEFT SEMI JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE users.uid = 1
) WHERE explain ilike '%Type:%' OR explain ilike '%Strictness%' OR explain ilike '%filter column%';

SELECT '--';

SELECT explain FROM (
  EXPLAIN actions = 1 SELECT * FROM users RIGHT ANY JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE users.uid = 1
) WHERE explain ilike '%Type:%' OR explain ilike '%Strictness%' OR explain ilike '%filter column%';

SELECT '--';

SELECT explain FROM (
  EXPLAIN actions = 1 SELECT * FROM users RIGHT SEMI JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE t2.number = 1
) WHERE explain ilike '%Type:%' OR explain ilike '%Strictness%' OR explain ilike '%filter column%';
