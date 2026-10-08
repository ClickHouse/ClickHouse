SET enable_lightweight_update=1;

DROP TABLE IF EXISTS lwu_on_fly;

CREATE TABLE lwu_on_fly (id UInt64, u UInt64, s String)
ENGINE = MergeTree
ORDER BY id PARTITION BY id % 2
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 1;

SYSTEM STOP MERGES lwu_on_fly;

INSERT INTO lwu_on_fly SELECT number, number, 'c' || number FROM numbers(10);
UPDATE lwu_on_fly SET u = 0 WHERE id % 3 = 2;

ALTER TABLE lwu_on_fly DELETE WHERE id IN (4, 5) SETTINGS mutations_sync = 0;
UPDATE lwu_on_fly SET u = 0 WHERE id % 3 = 1;

SELECT * FROM lwu_on_fly ORDER BY id settings apply_mutations_on_fly = 1, apply_patch_parts = 1;

ALTER TABLE lwu_on_fly DELETE WHERE 1 SETTINGS mutations_sync = 0;

SELECT count() FROM lwu_on_fly WHERE NOT ignore(*) SETTINGS apply_mutations_on_fly = 1, apply_patch_parts = 1;

DROP TABLE IF EXISTS lwu_on_fly;
