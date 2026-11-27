-- An ALTER that rewrites data must snapshot its byte weight when it is submitted, as an UPDATE does:
-- nothing reads system.mutations before a part leaves the scope, so a late snapshot sees only the rest.

SET mutations_sync = 0, alter_sync = 0;

DROP TABLE IF EXISTS t_alter_baseline SYNC;
CREATE TABLE t_alter_baseline (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_alter_baseline;

INSERT INTO t_alter_baseline SELECT number, number FROM numbers(100000);
INSERT INTO t_alter_baseline SELECT number, number FROM numbers(1000);

ALTER TABLE t_alter_baseline MODIFY COLUMN v String;
ALTER TABLE t_alter_baseline DROP PART 'all_1_1_0';

SELECT parts_to_do, progress > 0.5 FROM system.mutations
WHERE database = currentDatabase() AND table = 't_alter_baseline' AND NOT is_done;

DROP TABLE t_alter_baseline SYNC;
