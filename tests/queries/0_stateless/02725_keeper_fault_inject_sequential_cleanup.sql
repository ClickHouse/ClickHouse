DROP TABLE IF EXISTS keeper_fault_inject_sequential_cleanup;

CREATE TABLE keeper_fault_inject_sequential_cleanup (d Int8) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_02725/tables/keeper_fault_inject_sequential_cleanup', '1') ORDER BY d;

INSERT INTO keeper_fault_inject_sequential_cleanup VALUES (1);
INSERT INTO keeper_fault_inject_sequential_cleanup SETTINGS insert_deduplicate = 0 VALUES (1);
INSERT INTO keeper_fault_inject_sequential_cleanup SETTINGS insert_deduplicate = 0, insert_keeper_fault_injection_probability = 0.4, insert_keeper_fault_injection_seed = 5619964844601345291 VALUES (1);

-- with database ordinary it produced a warning
DROP TABLE keeper_fault_inject_sequential_cleanup;
