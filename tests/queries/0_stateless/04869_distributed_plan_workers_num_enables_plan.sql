-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Stateless Workers only run the stages of a distributed plan, so a non-zero
-- `distributed_plan_workers_num` enables `make_distributed_plan` on its own (issue #114501).
-- Everything runs locally so that the implied plan never dials a Worker.

SET distributed_plan_execute_locally = 1;

DROP TABLE IF EXISTS t_dp_workers;
CREATE TABLE t_dp_workers (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dp_workers SELECT number, number FROM numbers(100000);

SELECT 'off while the workers num is zero';
SELECT getSetting('make_distributed_plan');

SELECT 'implied by a per-query workers num';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3;

SELECT 'the implied plan still adjusts the settings it does not support';
SELECT getSetting('compile_expressions') SETTINGS distributed_plan_workers_num = 3, compile_expressions = 1;

SELECT 'implied by a session workers num';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan');

SELECT 'the query distributes with make_distributed_plan left alone';
-- sum() and not a bare count() so the trivial-count optimization cannot fold the plan away.
SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT sum(v) FROM t_dp_workers)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

DROP TABLE t_dp_workers;

SELECT 'an explicit value wins over the implication';
SET make_distributed_plan = 0;
SELECT getSetting('make_distributed_plan');

SELECT 'and keeps winning when the workers num changes again';
SET distributed_plan_workers_num = 5;
SELECT getSetting('make_distributed_plan');
