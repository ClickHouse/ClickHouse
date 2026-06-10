-- Provoking test for the borrowed-ThreadGroup use-after-free.
--
-- A materialized view runs its target insert under a "borrowed" child ThreadGroup that holds raw
-- pointers into the source query's group (memory_tracker / performance_counters). The per-table
-- MergeTree deduplication-log writer captures whichever group is current when it is (re)created and
-- pins it for the buffer's lifetime. A tiny dedup window forces the writer to rotate on every part,
-- so the source-side writer frees the query group while the MV-target writer is still attaching a
-- borrowed child that names it as parent -> the later parent-chain walk reads freed memory.
--
-- Under a debug/sanitizer build this surfaces via the borrowed-ThreadGroup lifetime checker (and the
-- traverse-delay amplifier makes the ASan UAF reliable). Under a release build the instruments are
-- compiled out and this is just a small MV-insert workload that prints "done".

SET parallel_view_processing = 1;

DROP TABLE IF EXISTS bt_uaf_src;
DROP TABLE IF EXISTS bt_uaf_dst;
DROP VIEW IF EXISTS bt_uaf_mv;

CREATE TABLE bt_uaf_src (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS non_replicated_deduplication_window = 1, min_bytes_for_wide_part = 0;

CREATE TABLE bt_uaf_dst (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS non_replicated_deduplication_window = 1, min_bytes_for_wide_part = 0;

CREATE MATERIALIZED VIEW bt_uaf_mv TO bt_uaf_dst AS SELECT x FROM bt_uaf_src;

INSERT INTO bt_uaf_src VALUES (1);
INSERT INTO bt_uaf_src VALUES (2);
INSERT INTO bt_uaf_src VALUES (3);
INSERT INTO bt_uaf_src VALUES (4);
INSERT INTO bt_uaf_src VALUES (5);
INSERT INTO bt_uaf_src VALUES (6);
INSERT INTO bt_uaf_src VALUES (7);
INSERT INTO bt_uaf_src VALUES (8);

DROP VIEW bt_uaf_mv;
DROP TABLE bt_uaf_dst;
DROP TABLE bt_uaf_src;

SELECT 'done';
