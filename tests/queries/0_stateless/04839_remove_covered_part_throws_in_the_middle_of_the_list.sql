-- Tags: no-parallel, no-replicated-database, no-async-insert, no-ordinary-database
-- no-parallel: SYSTEM ENABLE FAILPOINT is server-global, so a concurrent test's merge would hit the injected failure
-- no-replicated-database: failpoints are per-server and the test depends on local, non-replicated commit behaviour
-- no-async-insert: the test needs one part per INSERT so that the merge has several covered parts
-- no-ordinary-database: the test uses transactions

DROP TABLE IF EXISTS remove_covered_part_failed;

CREATE TABLE remove_covered_part_failed (n Int64)
    ENGINE = MergeTree ORDER BY n
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;


INSERT INTO remove_covered_part_failed VALUES (1);
INSERT INTO remove_covered_part_failed VALUES (2);
INSERT INTO remove_covered_part_failed VALUES (3);
INSERT INTO remove_covered_part_failed VALUES (4);

SELECT partition_id, name, removal_csn, removal_tid = (0, 0, '00000000-0000-0000-0000-000000000000')
        FROM system.parts WHERE table = 'remove_covered_part_failed'
            AND database = currentDatabase() and active
        ORDER BY name;

SYSTEM ENABLE FAILPOINT add_new_part_and_remove_covered_non_tx_second_part;

OPTIMIZE TABLE remove_covered_part_failed FINAL; -- { serverError SERIALIZATION_ERROR }

SYSTEM DISABLE FAILPOINT add_new_part_and_remove_covered_non_tx_second_part;

SELECT partition_id, name, removal_csn, removal_tid = (0, 0, '00000000-0000-0000-0000-000000000000')
        FROM system.parts WHERE table = 'remove_covered_part_failed'
            AND database = currentDatabase() and active
        ORDER BY name;
SELECT count() FROM remove_covered_part_failed;
BEGIN TRANSACTION;
SELECT count() FROM remove_covered_part_failed;
COMMIT;
