-- Tags: no-parallel, no-fasttest, no-shared-merge-tree, no-replicated-database
-- no-parallel: the failpoint is server-wide and pauses the first mutation task of any table.
-- no-replicated-database: the failpoint is in the task of a non-replicated MergeTree.

-- A mutation task that was selected before KILL MUTATION but starts after it must not apply the killed mutation.

DROP TABLE IF EXISTS t_kill_single;
DROP TABLE IF EXISTS t_kill_squashed;

CREATE TABLE t_kill_single (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_kill_single SELECT number, number FROM numbers(10);

SYSTEM STOP MERGES t_kill_single;
ALTER TABLE t_kill_single DELETE WHERE id < 5 SETTINGS mutations_sync = 0;
SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_before_merge_list;
SYSTEM START MERGES t_kill_single;
SYSTEM WAIT FAILPOINT mt_mutate_task_pause_before_merge_list PAUSE;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_kill_single' FORMAT Null;
SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_merge_list;

-- Returns once the killed task has given the part back.
ALTER TABLE t_kill_single UPDATE x = x + 100 WHERE 1 SETTINGS mutations_sync = 1;
SELECT count(), sum(x) FROM t_kill_single;

-- Two mutations in one task, only the first one is killed.
CREATE TABLE t_kill_squashed (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_kill_squashed SELECT number, number FROM numbers(10);

SYSTEM STOP MERGES t_kill_squashed;
ALTER TABLE t_kill_squashed DELETE WHERE id < 5 SETTINGS mutations_sync = 0;
ALTER TABLE t_kill_squashed UPDATE x = x + 100 WHERE 1 SETTINGS mutations_sync = 0;
SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_before_merge_list;
SYSTEM START MERGES t_kill_squashed;
SYSTEM WAIT FAILPOINT mt_mutate_task_pause_before_merge_list PAUSE;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_kill_squashed' AND command LIKE '%DELETE%' FORMAT Null;

-- Pause the retry of the UPDATE: the killed task has reported its result by then.
SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_before_merge_list;
SYSTEM NOTIFY FAILPOINT mt_mutate_task_pause_before_merge_list;
SYSTEM WAIT FAILPOINT mt_mutate_task_pause_before_merge_list PAUSE;
-- A `mutations_sync` client waiting for the UPDATE fails while it has a fail reason.
SELECT command, is_done, latest_fail_reason = '' FROM system.mutations WHERE database = currentDatabase() AND table = 't_kill_squashed';
SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_merge_list;

ALTER TABLE t_kill_squashed UPDATE x = x + 1000 WHERE 1 SETTINGS mutations_sync = 1;
SELECT count(), sum(x) FROM t_kill_squashed;

SYSTEM FLUSH LOGS part_log;
SELECT table, errorCodeToName(argMin(error, event_time_microseconds)), countIf(error != 0)
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MutatePart'
GROUP BY table ORDER BY table;

DROP TABLE t_kill_single;
DROP TABLE t_kill_squashed;
