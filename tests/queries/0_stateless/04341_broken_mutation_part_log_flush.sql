-- A synchronous mutation (mutations_sync) that fails must queue its failed MutatePart
-- part_log entry before reporting the failure to the client, otherwise an immediate
-- SYSTEM FLUSH LOGS part_log can miss that row.

DROP TABLE IF EXISTS t_failed_mutation_part_log;

CREATE TABLE t_failed_mutation_part_log (x UInt64) ENGINE = MergeTree ORDER BY tuple();

SET mutations_sync = 2;

INSERT INTO t_failed_mutation_part_log SELECT number FROM numbers(10);

ALTER TABLE t_failed_mutation_part_log UPDATE x = x + throwIf(1) WHERE 1; -- { serverError UNFINISHED }

SYSTEM FLUSH LOGS part_log;

SELECT count() > 0
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND database = currentDatabase() AND table = 't_failed_mutation_part_log'
  AND event_type = 'MutatePart' AND error != 0;

DROP TABLE IF EXISTS t_failed_mutation_part_log;
