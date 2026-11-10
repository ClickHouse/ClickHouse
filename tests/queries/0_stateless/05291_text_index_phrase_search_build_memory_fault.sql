-- Tags: no-parallel-replicas

-- An INSERT into a table with a phrase search text index fails with MEMORY_LIMIT_EXCEEDED, and the server survives,
-- when allocations fail while the index is being built.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS allow_experimental_text_index_phrase_search = 1;

-- Every row is a new token, so most tracked allocations of each INSERT happen while the index is built.
-- One thread: a fault while scheduling pipeline threads would fail with CANNOT_SCHEDULE_TASK instead.
INSERT INTO tab SETTINGS memory_tracker_fault_probability = 0.001, max_untracked_memory = 0, max_threads = 1 SELECT concat('token', toString(number)) FROM numbers(20000); -- { serverError MEMORY_LIMIT_EXCEEDED }
INSERT INTO tab SETTINGS memory_tracker_fault_probability = 0.001, max_untracked_memory = 0, max_threads = 1 SELECT concat('token', toString(number)) FROM numbers(20000); -- { serverError MEMORY_LIMIT_EXCEEDED }
INSERT INTO tab SETTINGS memory_tracker_fault_probability = 0.001, max_untracked_memory = 0, max_threads = 1 SELECT concat('token', toString(number)) FROM numbers(20000); -- { serverError MEMORY_LIMIT_EXCEEDED }
INSERT INTO tab SETTINGS memory_tracker_fault_probability = 0.001, max_untracked_memory = 0, max_threads = 1 SELECT concat('token', toString(number)) FROM numbers(20000); -- { serverError MEMORY_LIMIT_EXCEEDED }
INSERT INTO tab SETTINGS memory_tracker_fault_probability = 0.001, max_untracked_memory = 0, max_threads = 1 SELECT concat('token', toString(number)) FROM numbers(20000); -- { serverError MEMORY_LIMIT_EXCEEDED }

TRUNCATE TABLE tab;
INSERT INTO tab SELECT concat('token', toString(number)) FROM numbers(20000);
SELECT count() FROM tab WHERE hasPhrase(s, 'token123');

DROP TABLE tab;
