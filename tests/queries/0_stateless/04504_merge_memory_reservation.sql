-- Merges reserve the memory of their input/output IO buffers up front (see MergeMemoryReservation).
-- This test checks that the reservation does not break normal merges and that the observability metric is wired.

DROP TABLE IF EXISTS t_merge_mem_reservation;

CREATE TABLE t_merge_mem_reservation (a UInt64, s String, arr Array(UInt64))
ENGINE = MergeTree ORDER BY a;

SYSTEM STOP MERGES t_merge_mem_reservation;

INSERT INTO t_merge_mem_reservation SELECT number, toString(number), range(number % 10) FROM numbers(1000);
INSERT INTO t_merge_mem_reservation SELECT number, toString(number), range(number % 10) FROM numbers(1000, 1000);
INSERT INTO t_merge_mem_reservation SELECT number, toString(number), range(number % 10) FROM numbers(2000, 1000);

-- At least three parts, none merged yet (an INSERT may be split into more than one part).
SELECT count() >= 3 FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_reservation' AND active;

SYSTEM START MERGES t_merge_mem_reservation;

OPTIMIZE TABLE t_merge_mem_reservation FINAL;

-- The merge preserved all rows and produced a single part.
SELECT count() FROM t_merge_mem_reservation;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_reservation' AND active;

-- The reservation is released once merges finish; the metric never goes negative.
SELECT value >= 0 FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';

DROP TABLE t_merge_mem_reservation;
