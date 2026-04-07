DROP TABLE IF EXISTS skip_idx_comp_parts;
CREATE TABLE skip_idx_comp_parts (a Int, b Int, index b_idx b TYPE minmax GRANULARITY 4)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity=256, index_granularity_bytes = '10Mi', merge_max_block_size=100;

SYSTEM STOP MERGES skip_idx_comp_parts;

INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);
INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);
INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);
INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);

SYSTEM START MERGES skip_idx_comp_parts;
OPTIMIZE TABLE skip_idx_comp_parts FINAL;

SELECT count() FROM skip_idx_comp_parts WHERE b > 100;

DROP TABLE skip_idx_comp_parts;
