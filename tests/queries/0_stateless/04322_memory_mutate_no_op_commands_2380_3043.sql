-- Tags: no-replicated-database
-- Reason: replicated database does not support `Memory` engine via the same path.

DROP TABLE IF EXISTS t_mem_2380;
CREATE TABLE t_mem_2380 (id UInt64, value String) ENGINE = Memory;

INSERT INTO t_mem_2380 VALUES (1, 'a'), (2, 'b');
INSERT INTO t_mem_2380 VALUES (3, 'c'), (4, 'd');

SELECT 'before:', count() FROM t_mem_2380;

-- Parenthesised form (as emitted by the BuzzHouse fuzzer)
ALTER TABLE t_mem_2380 (APPLY PATCHES);
SELECT 'after parens:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- Bare form
ALTER TABLE t_mem_2380 APPLY PATCHES;
SELECT 'after bare:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- APPLY DELETED MASK is a no-op on `Memory`.
ALTER TABLE t_mem_2380 APPLY DELETED MASK;
SELECT 'after deleted mask:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- MATERIALIZE STATISTICS ALL is a no-op on `Memory`.
ALTER TABLE t_mem_2380 MATERIALIZE STATISTICS ALL;
SELECT 'after stats all:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- MATERIALIZE INDEX on a non-existent index is a no-op on `Memory`.
ALTER TABLE t_mem_2380 MATERIALIZE INDEX nonex SETTINGS send_logs_level = 'error';
SELECT 'after mat index:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- MATERIALIZE PROJECTION on a non-existent projection is a no-op on `Memory`.
ALTER TABLE t_mem_2380 MATERIALIZE PROJECTION nonex SETTINGS send_logs_level = 'error';
SELECT 'after mat proj:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- REWRITE PARTS is a no-op on `Memory` (no parts to rewrite).
ALTER TABLE t_mem_2380 REWRITE PARTS;
SELECT 'after rewrite parts:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

-- Mixed: APPLY PATCHES alongside a real UPDATE must still execute the UPDATE.
ALTER TABLE t_mem_2380 UPDATE value = upper(value) WHERE id = 1, APPLY PATCHES SETTINGS mutations_sync = 1;
SELECT 'after mixed:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_mem_2380 ORDER BY id);

DROP TABLE t_mem_2380;
