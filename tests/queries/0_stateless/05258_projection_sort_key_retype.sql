-- Tags: no-parallel-replicas
-- no-parallel-replicas: forced projection reads are not planned under parallel replicas.

-- After ALTER TABLE ... MODIFY COLUMN changes the type of a column a projection is ordered by, queries
-- through the projection return the same rows as the table, and the projection is rebuilt, not dropped.

SET optimize_use_projections = 1, use_query_condition_cache = 0, mutations_sync = 2, alter_sync = 2;

-- The table settings keep the old data as written (no statistics, sparse columns or key cache) and the index layout fixed.

-- The parts were written before the column existed; the projection was materialized from its DEFAULT.
CREATE TABLE ta (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO ta SELECT number FROM numbers(20000);
ALTER TABLE ta ADD COLUMN v UInt32 DEFAULT 7;
ALTER TABLE ta ADD PROJECTION p (SELECT id, v ORDER BY v);
ALTER TABLE ta MATERIALIZE PROJECTION p;
SELECT 'A part type', part_type FROM system.parts WHERE database = currentDatabase() AND table = 'ta' AND active;
SELECT 'A projection marks', marks FROM system.projection_parts WHERE database = currentDatabase() AND table = 'ta' AND name = 'p' AND active;
ALTER TABLE ta MODIFY COLUMN v Nullable(UInt32) DEFAULT 7;
SELECT 'A projection', count() FROM ta WHERE v > 5 SETTINGS force_optimize_projection = 1;
SELECT 'A table', count() FROM ta WHERE v > 5 SETTINGS optimize_use_projections = 0;
SELECT 'A projection type', type FROM system.projection_parts_columns WHERE database = currentDatabase() AND table = 'ta' AND column = 'v' AND active;

-- The same on a compact part, which stores the column.
CREATE TABLE ta2 (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO ta2 SELECT number FROM numbers(4);
ALTER TABLE ta2 ADD COLUMN v UInt32 DEFAULT 7;
ALTER TABLE ta2 ADD PROJECTION p (SELECT id, v ORDER BY v);
ALTER TABLE ta2 MATERIALIZE PROJECTION p;
SELECT 'A2 part type', part_type FROM system.parts WHERE database = currentDatabase() AND table = 'ta2' AND active;
SELECT 'A2 part stores v', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 'ta2' AND column = 'v' AND active;
ALTER TABLE ta2 MODIFY COLUMN v Nullable(UInt32) DEFAULT 7;
SELECT 'A2 projection', count() FROM ta2 WHERE v > 5 SETTINGS force_optimize_projection = 1;
SELECT 'A2 projection type', type FROM system.projection_parts_columns WHERE database = currentDatabase() AND table = 'ta2' AND column = 'v' AND active;

-- While the conversion mutation is pending, and after it has finished.
CREATE TABLE tp (i Int32, j Int32, PROJECTION p (SELECT * ORDER BY j)) ENGINE = MergeTree ORDER BY i
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO tp SELECT number, number FROM numbers(1000);
ALTER TABLE tp DELETE WHERE i < 0;
SYSTEM STOP MERGES tp;
ALTER TABLE tp MODIFY COLUMN j Nullable(Int32) SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'D pending', count() FROM tp WHERE j = 6;
SELECT count() FROM tp WHERE j = 6 SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
SELECT count() FROM mergeTreeProjection(currentDatabase(), tp, p) WHERE j = 6; -- { serverError NOT_IMPLEMENTED }
SYSTEM START MERGES tp;
ALTER TABLE tp DELETE WHERE i < 0 SETTINGS mutations_sync = 2;
SELECT 'D projection', count() FROM tp WHERE j = 6 SETTINGS force_optimize_projection = 1;
SELECT 'D mergeTreeProjection', count() FROM mergeTreeProjection(currentDatabase(), tp, p) WHERE j = 6;
SELECT 'D projection type', type FROM system.projection_parts_columns WHERE database = currentDatabase() AND table = 'tp' AND column = 'j' AND active;

-- A killed conversion leaves the projection in the old type; MATERIALIZE PROJECTION repairs it.
CREATE TABLE te1 (id UInt64, v UInt32, PROJECTION p (SELECT id, v ORDER BY v)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO te1 VALUES (1, 9), (2, 10), (3, 11);
ALTER TABLE te1 DELETE WHERE id = 0;
SYSTEM STOP MERGES te1;
ALTER TABLE te1 MODIFY COLUMN v String SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 'te1' SYNC FORMAT Null;
SELECT 'E1 killed', count() FROM te1 WHERE v = '10';
SYSTEM START MERGES te1;
ALTER TABLE te1 MATERIALIZE PROJECTION p;
SELECT 'E1 materialized', count() FROM te1 WHERE v = '10' SETTINGS force_optimize_projection = 1;

-- A merge rebuilds such a projection in the new order instead of merging it through.
CREATE TABLE te2 (id UInt64, v UInt32, PROJECTION p (SELECT id, v ORDER BY v)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO te2 VALUES (1, 9), (2, 10);
INSERT INTO te2 VALUES (3, 11);
ALTER TABLE te2 DELETE WHERE id = 0;
SYSTEM STOP MERGES te2;
ALTER TABLE te2 MODIFY COLUMN v String SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 'te2' SYNC FORMAT Null;
SYSTEM START MERGES te2;
OPTIMIZE TABLE te2 FINAL;
SELECT 'E2 order', v FROM te2 ORDER BY v SETTINGS force_optimize_projection = 1, optimize_read_in_order = 1;
SELECT 'E2 projection', count() FROM te2 WHERE v = '10' SETTINGS force_optimize_projection = 1;

-- Changes that keep the stored values valid keep the projection in use: a wider Enum, a time zone of a plain key.
CREATE TABLE tf1 (id UInt64, e Enum8('a' = 1, 'b' = 2), PROJECTION p (SELECT id, e ORDER BY e)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO tf1 SELECT number, if(number % 2 = 0, 'a', 'b') FROM numbers(10);
ALTER TABLE tf1 DELETE WHERE id = 1000;
ALTER TABLE tf1 MODIFY COLUMN e Enum8('a' = 1, 'b' = 2, 'c' = 3);
SELECT 'F1 projection', count() FROM tf1 WHERE e = 'b' SETTINGS force_optimize_projection = 1;

CREATE TABLE tf2 (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, dt ORDER BY dt)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO tf2 SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24);
ALTER TABLE tf2 DELETE WHERE id = 1000;
ALTER TABLE tf2 MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT 'F2 projection', count() FROM tf2 WHERE dt >= toDateTime('2026-01-01 05:00:00', 'UTC') SETTINGS force_optimize_projection = 1;
SELECT 'F2 table', count() FROM tf2 WHERE dt >= toDateTime('2026-01-01 05:00:00', 'UTC') SETTINGS optimize_use_projections = 0;

-- A time zone change moves every value of a projection ordered by toHour(dt).
CREATE TABLE tg (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, dt ORDER BY toHour(dt))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 1, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO tg SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24);
ALTER TABLE tg DELETE WHERE id = 1000;
ALTER TABLE tg MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT 'G table', count() FROM tg WHERE toHour(dt) = 5 SETTINGS optimize_use_projections = 0;
SELECT 'G default', count() FROM tg WHERE toHour(dt) = 5;
SELECT count() FROM tg WHERE toHour(dt) = 5 SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
ALTER TABLE tg MATERIALIZE PROJECTION p;
SELECT 'G materialized', count() FROM tg WHERE toHour(dt) = 5 SETTINGS force_optimize_projection = 1;

-- With the primary key loaded eagerly, such a projection is broken after a restart; the next mutation rebuilds it.
CREATE TABLE th (id UInt64, v UInt32, PROJECTION p (SELECT id, v ORDER BY v)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1, primary_key_lazy_load = 0;
INSERT INTO th VALUES (1, 9), (2, 10), (3, 11);
ALTER TABLE th DELETE WHERE id = 0;
SYSTEM STOP MERGES th;
ALTER TABLE th MODIFY COLUMN v String SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 'th' SYNC FORMAT Null;
DETACH TABLE th;
ATTACH TABLE th;
SELECT 'H broken', is_broken FROM system.projection_parts WHERE database = currentDatabase() AND table = 'th' AND name = 'p' AND active;
SELECT 'H table', count() FROM th WHERE v = '10';
SYSTEM START MERGES th;
ALTER TABLE th DELETE WHERE id = 0;
SELECT 'H projection', count() FROM th WHERE v = '10' SETTINGS force_optimize_projection = 1;

-- ... and a merge rebuilds it instead of leaving it out of the merged part.
CREATE TABLE th2 (id UInt64, v UInt32, PROJECTION p (SELECT id, v ORDER BY v)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    enable_block_number_column = 0, enable_block_offset_column = 0, use_primary_key_cache = 0, prewarm_primary_key_cache = 0,
    auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1, primary_key_lazy_load = 0;
INSERT INTO th2 VALUES (1, 9), (2, 10);
INSERT INTO th2 VALUES (3, 11);
ALTER TABLE th2 DELETE WHERE id = 0;
SYSTEM STOP MERGES th2;
ALTER TABLE th2 MODIFY COLUMN v String SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 'th2' SYNC FORMAT Null;
DETACH TABLE th2;
ATTACH TABLE th2;
SYSTEM START MERGES th2;
OPTIMIZE TABLE th2 FINAL;
SELECT 'H2 projection', count() FROM th2 WHERE v = '10' SETTINGS force_optimize_projection = 1;
SELECT 'H2 projection parts', count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 'th2' AND name = 'p' AND active;
