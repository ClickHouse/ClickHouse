SET use_uncompressed_cache = 0;

DROP TABLE IF EXISTS adaptive_table;

--- If granularity of consequent blocks differs a lot, then adaptive
--- granularity will adjust amout of marks correctly. Data for test empirically
--- derived, it's quite hard to get good parameters.

CREATE TABLE adaptive_table(
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY key
SETTINGS index_granularity_bytes=1048576, min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0;

SET max_block_size=900;

-- There are about 900 marks for our settings.
INSERT INTO adaptive_table SELECT number, if(number > 700, randomPrintableASCII(102400), randomPrintableASCII(1)) FROM numbers(10000);

OPTIMIZE TABLE adaptive_table FINAL;

SELECT marks FROM system.parts WHERE table = 'adaptive_table' and database=currentDatabase() and active;

SET enable_filesystem_cache = 0;

-- If we have computed granularity incorrectly than we will exceed this limit.
SET max_memory_usage='30M';

SELECT max(length(value)) FROM adaptive_table;

DROP TABLE IF EXISTS adaptive_table;
