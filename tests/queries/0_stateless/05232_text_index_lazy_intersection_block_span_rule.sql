-- The lazy AND over bitpacked posting lists chooses between leapfrog and the brute-force counting pass.
-- Leapfrog pays off only when the sparsest list can skip whole packed blocks (128 postings) of the
-- densest one, i.e. when `min_density * 128 < max_density`; otherwise every block is decoded anyway
-- and leapfrog only adds a search per posting. This test pins that rule for the default `auto` and for
-- the two forcing values of `text_index_postings_intersection_algorithm`.

SET enable_full_text_index = 1;
SET text_index_posting_list_apply_mode = 'lazy';
-- The `_default` queries below assert what the `auto` rule picks, so pin the algorithm against
-- the settings randomizer instead of relying on the default value.
SET text_index_postings_intersection_algorithm = 'auto';
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS tab_lazy_rule;

-- posting_list_block_size = 256: tokens with more than 256 postings take the multi-segment lazy cursor
-- path, `dsparse` (2 postings) is embedded. A single granule keeps every pair in one intersection call.
--   adense  : every row 0..7999      -> density 1.0
--   bmid    : number % 10 = 0        -> 800 docs, density 0.1
--   cmid    : number % 8 = 0         -> 1000 docs, density 0.125
--   dsparse : number % 4000 = 0      -> 2 docs, density 0.0005
CREATE TABLE tab_lazy_rule(
    k UInt64,
    s String,
    INDEX idx s TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_block_size = 256))
ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M';

INSERT INTO tab_lazy_rule
SELECT number,
    concat(
        'adense',
        if(number % 10 = 0, ' bmid', ''),
        if(number % 8 = 0, ' cmid', ''),
        if(number % 4000 = 0, ' dsparse', ''))
FROM numbers(8000);

-- Two mid-density lists (0.1 and 0.125): both below the 0.2 threshold, but 0.1 * 128 >= 0.125,
-- so the sparsest list has a posting in every block of the densest one -> brute force.
SELECT count() FROM tab_lazy_rule WHERE hasAllTokens(s, ['bmid', 'cmid'])
    SETTINGS log_comment = '05232_rule_mid_pair_default';

-- Sparse against dense (0.0005 * 128 < 1.0): leapfrog can skip almost every block.
SELECT count() FROM tab_lazy_rule WHERE hasAllTokens(s, ['adense', 'dsparse'])
    SETTINGS log_comment = '05232_rule_sparse_dense_default';

-- 'leapfrog' forces leapfrog even where the rule would pick brute force.
SELECT count() FROM tab_lazy_rule WHERE hasAllTokens(s, ['bmid', 'cmid'])
    SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '05232_rule_mid_pair_leapfrog';

-- 'bruteforce' forces brute force even where the rule would pick leapfrog.
SELECT count() FROM tab_lazy_rule WHERE hasAllTokens(s, ['adense', 'dsparse'])
    SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '05232_rule_sparse_dense_bruteforce';

SYSTEM FLUSH LOGS query_log;

-- Under parallel replicas the counters land on the replica rows; resolve the initiator rows by
-- `current_database` and aggregate every row of the same `initial_query_id`.
WITH initial_queries AS
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND is_initial_query = 1
      AND log_comment LIKE '05232_rule_%'
)
SELECT
    iq.log_comment AS tag,
    sum(ql.ProfileEvents['TextIndexLazyBruteForceIntersections']) > 0 AS brute_force,
    sum(ql.ProfileEvents['TextIndexLazyLeapfrogIntersections']) > 0 AS leapfrog
FROM system.query_log AS ql
INNER JOIN initial_queries AS iq ON ql.initial_query_id = iq.query_id
WHERE ql.event_date >= yesterday() AND ql.event_time >= now() - 600
  AND ql.type = 'QueryFinish'
GROUP BY tag
ORDER BY tag;

DROP TABLE tab_lazy_rule;
