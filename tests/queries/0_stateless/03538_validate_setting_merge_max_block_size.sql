DROP TABLE IF EXISTS validate_setting_merge_max_block_size;

CREATE TABLE validate_setting_merge_max_block_size (x Int64) ENGINE = MergeTree() ORDER BY tuple() SETTINGS merge_max_block_size = 0; -- {serverError BAD_ARGUMENTS}
CREATE TABLE validate_setting_merge_max_block_size (x Int64) ENGINE = MergeTree() ORDER BY tuple() SETTINGS merge_max_block_size = 1;
ALTER TABLE validate_setting_merge_max_block_size MODIFY SETTING merge_max_block_size = 0; -- {serverError BAD_ARGUMENTS}

DROP TABLE validate_setting_merge_max_block_size;
