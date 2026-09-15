DROP TABLE IF EXISTS stream_rebuilt_projection_replacing_compact_part_format;

-- A reducing merge can have a large source-row upper bound but a tiny result.
-- Keep the projection deliberately wide so choosing `Wide` from the source count
-- would add marks and files without adding rows.
CREATE TABLE stream_rebuilt_projection_replacing_compact_part_format
(
    k UInt64,
    version UInt64,
    c001 UInt64,
    c002 UInt64,
    c003 UInt64,
    c004 UInt64,
    c005 UInt64,
    c006 UInt64,
    c007 UInt64,
    c008 UInt64,
    c009 UInt64,
    c010 UInt64,
    c011 UInt64,
    c012 UInt64,
    c013 UInt64,
    c014 UInt64,
    c015 UInt64,
    c016 UInt64,
    c017 UInt64,
    c018 UInt64,
    c019 UInt64,
    c020 UInt64,
    c021 UInt64,
    c022 UInt64,
    c023 UInt64,
    c024 UInt64,
    c025 UInt64,
    c026 UInt64,
    c027 UInt64,
    c028 UInt64,
    c029 UInt64,
    c030 UInt64,
    c031 UInt64,
    c032 UInt64,
    c033 UInt64,
    c034 UInt64,
    c035 UInt64,
    c036 UInt64,
    c037 UInt64,
    c038 UInt64,
    c039 UInt64,
    c040 UInt64,
    c041 UInt64,
    c042 UInt64,
    c043 UInt64,
    c044 UInt64,
    c045 UInt64,
    c046 UInt64,
    c047 UInt64,
    c048 UInt64,
    c049 UInt64,
    c050 UInt64,
    c051 UInt64,
    c052 UInt64,
    c053 UInt64,
    c054 UInt64,
    c055 UInt64,
    c056 UInt64,
    c057 UInt64,
    c058 UInt64,
    c059 UInt64,
    c060 UInt64,
    c061 UInt64,
    c062 UInt64,
    c063 UInt64,
    c064 UInt64,
    c065 UInt64,
    c066 UInt64,
    c067 UInt64,
    c068 UInt64,
    c069 UInt64,
    c070 UInt64,
    c071 UInt64,
    c072 UInt64,
    c073 UInt64,
    c074 UInt64,
    c075 UInt64,
    c076 UInt64,
    c077 UInt64,
    c078 UInt64,
    c079 UInt64,
    c080 UInt64,
    c081 UInt64,
    c082 UInt64,
    c083 UInt64,
    c084 UInt64,
    c085 UInt64,
    c086 UInt64,
    c087 UInt64,
    c088 UInt64,
    c089 UInt64,
    c090 UInt64,
    c091 UInt64,
    c092 UInt64,
    c093 UInt64,
    c094 UInt64,
    c095 UInt64,
    c096 UInt64,
    c097 UInt64,
    c098 UInt64,
    c099 UInt64,
    c100 UInt64,
    c101 UInt64,
    c102 UInt64,
    c103 UInt64,
    c104 UInt64,
    c105 UInt64,
    c106 UInt64,
    c107 UInt64,
    c108 UInt64,
    c109 UInt64,
    c110 UInt64,
    c111 UInt64,
    c112 UInt64,
    c113 UInt64,
    c114 UInt64,
    c115 UInt64,
    c116 UInt64,
    c117 UInt64,
    c118 UInt64,
    c119 UInt64,
    c120 UInt64,
    c121 UInt64,
    c122 UInt64,
    c123 UInt64,
    c124 UInt64,
    c125 UInt64,
    c126 UInt64,
    c127 UInt64,
    c128 UInt64,
    c129 UInt64,
    c130 UInt64,
    c131 UInt64,
    c132 UInt64,
    c133 UInt64,
    c134 UInt64,
    c135 UInt64,
    c136 UInt64,
    c137 UInt64,
    c138 UInt64,
    c139 UInt64,
    c140 UInt64,
    c141 UInt64,
    c142 UInt64,
    c143 UInt64,
    c144 UInt64,
    c145 UInt64,
    c146 UInt64,
    c147 UInt64,
    c148 UInt64,
    c149 UInt64,
    c150 UInt64,
    c151 UInt64,
    c152 UInt64,
    c153 UInt64,
    c154 UInt64,
    c155 UInt64,
    c156 UInt64,
    c157 UInt64,
    c158 UInt64,
    c159 UInt64,
    c160 UInt64,
    c161 UInt64,
    c162 UInt64,
    c163 UInt64,
    c164 UInt64,
    c165 UInt64,
    c166 UInt64,
    c167 UInt64,
    c168 UInt64,
    c169 UInt64,
    c170 UInt64,
    c171 UInt64,
    c172 UInt64,
    c173 UInt64,
    c174 UInt64,
    c175 UInt64,
    c176 UInt64,
    c177 UInt64,
    c178 UInt64,
    c179 UInt64,
    c180 UInt64,
    c181 UInt64,
    c182 UInt64,
    c183 UInt64,
    c184 UInt64,
    c185 UInt64,
    c186 UInt64,
    c187 UInt64,
    c188 UInt64,
    c189 UInt64,
    c190 UInt64,
    c191 UInt64,
    c192 UInt64,
    c193 UInt64,
    c194 UInt64,
    c195 UInt64,
    c196 UInt64,
    c197 UInt64,
    c198 UInt64,
    c199 UInt64,
    c200 UInt64,
    PROJECTION p_normal (SELECT * ORDER BY k)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY k
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    deduplicate_merge_projection_mode = 'rebuild',
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 2,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_replacing_compact_part_format;
INSERT INTO stream_rebuilt_projection_replacing_compact_part_format VALUES (0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
INSERT INTO stream_rebuilt_projection_replacing_compact_part_format VALUES (0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2);

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_replacing_compact_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_replacing_compact_part_format FINAL;

SELECT part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_replacing_compact_part_format' AND name = 'p_normal' AND active;
SELECT count(), sum(c200)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_replacing_compact_part_format, p_normal);
CHECK TABLE stream_rebuilt_projection_replacing_compact_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_replacing_compact_part_format;
DROP TABLE IF EXISTS stream_rebuilt_projection_replacing_wide_part_format;

-- Conversely, a `ReplacingMergeTree` merge that keeps enough rows must promote the delayed
-- writer to `Wide` and replay its bounded `Native` prefix in projection order.
CREATE TABLE stream_rebuilt_projection_replacing_wide_part_format
(
    k UInt64,
    version UInt64,
    v UInt64,
    PROJECTION p_normal (SELECT k, v ORDER BY k)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY k
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    deduplicate_merge_projection_mode = 'rebuild',
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 24,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_replacing_wide_part_format;
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number, 1, number FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 3, 1, number + 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 6, 1, number + 6 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 9, 1, number + 9 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 12, 1, number + 12 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 15, 1, number + 15 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 18, 1, number + 18 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 21, 1, number + 21 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 24, 1, number + 24 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 27, 1, number + 27 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 30, 1, number + 30 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_replacing_wide_part_format SELECT number + 33, 1, number + 33 FROM numbers(3);

SYSTEM START MERGES stream_rebuilt_projection_replacing_wide_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_replacing_wide_part_format FINAL;

SELECT part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_replacing_wide_part_format' AND name = 'p_normal' AND active;
SELECT count(), sum(v)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_replacing_wide_part_format, p_normal);
CHECK TABLE stream_rebuilt_projection_replacing_wide_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_replacing_wide_part_format;
DROP TABLE IF EXISTS stream_rebuilt_projection_replacing_reordered_compact_part_format;

-- Once all projected rows are known, an unordered reducing rebuild whose row
-- bound still falls below the `Wide` threshold can replay its bounded sorted runs
-- straight into a `Compact` part. Exercise both normal and aggregate projections.
CREATE TABLE stream_rebuilt_projection_replacing_reordered_compact_part_format
(
    k UInt64,
    version UInt64,
    v UInt64,
    PROJECTION p_reordered (SELECT v, k ORDER BY v),
    PROJECTION p_reordered_aggregate (SELECT v % 2 AS g, sum(v) GROUP BY g)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY k
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    deduplicate_merge_projection_mode = 'rebuild',
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 3,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_replacing_reordered_compact_part_format;
INSERT INTO stream_rebuilt_projection_replacing_reordered_compact_part_format VALUES (0, 1, 4), (1, 1, 3);
INSERT INTO stream_rebuilt_projection_replacing_reordered_compact_part_format VALUES (0, 2, 2), (1, 2, 1);

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_replacing_reordered_compact_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_replacing_reordered_compact_part_format FINAL;

SELECT name, part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_replacing_reordered_compact_part_format' AND active
ORDER BY name;
SELECT count(), sum(v), sum(k)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_replacing_reordered_compact_part_format, p_reordered);
SELECT count(), sumMerge(`sum(v)`)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_replacing_reordered_compact_part_format, p_reordered_aggregate);
CHECK TABLE stream_rebuilt_projection_replacing_reordered_compact_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_replacing_reordered_compact_part_format;
