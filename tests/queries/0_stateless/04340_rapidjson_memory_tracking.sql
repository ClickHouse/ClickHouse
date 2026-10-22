-- Tags: no-fasttest
-- no-fasttest: rapidjson is required for prettyPrintJSON, JSONMergePatch and the rapidjson JSON parser.

-- Normal behaviour is unchanged.
SELECT prettyPrintJSON('{"a":1,"b":"hello"}');
SELECT JSONMergePatch('{"a":1}', '{"name": "joey"}', '{"name": "tom"}', '{"name": "zoey"}');
SELECT JSONLength('[1,2,3,4]') SETTINGS allow_simdjson = 0;

-- prettyPrintJSON: a tiny, deeply nested input pretty-prints to an enormous output (indentation
-- grows per level, so the output is on the order of tens of GiB). The rapidjson output buffer is
-- now accounted against the memory tracker, so this is rejected early (after only ~50 MiB) with a
-- clean MEMORY_LIMIT_EXCEEDED, which keeps the query cheap. Before the fix the buffer used a raw
-- allocator that bypassed the tracker, so the allocation grew unbounded and aborted the server
-- (tripping the sanitizer allocation-size cap, or running out of memory) instead.
SELECT length(prettyPrintJSON(concat(repeat('[', 40000), repeat(']', 40000)), 32))
SETTINGS max_memory_usage = 50000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- rapidjson JSON parser (allow_simdjson = 0): the DOM built while parsing a large untrusted
-- document is now accounted against the memory tracker, so this is rejected with
-- MEMORY_LIMIT_EXCEEDED. Before the fix the DOM used a raw allocator that bypassed the tracker.
SELECT JSONLength(concat('[0', repeat(',0', 1000000), repeat(',0', 1000000), ']'))
SETTINGS allow_simdjson = 0, max_memory_usage = 48000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- JSONMergePatch: the parsed DOM is now accounted against the memory tracker. A single large
-- object is rejected with MEMORY_LIMIT_EXCEEDED; the result length is tiny, so without the fix the
-- untracked DOM would have parsed successfully instead.
SELECT length(JSONMergePatch(concat('{"a":0', repeat(',"a":0', 1000000), '}')))
SETTINGS max_memory_usage = 24000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- The DOM is now rewound between rows, so one block's worth of documents no longer accumulates in
-- the parser's pool. Before that, the pool grew to max_block_size times one document's DOM and this
-- query could not run under this limit.
SELECT count() FROM zeros(65536)
WHERE NOT ignore(JSONExtractInt(materialize(concat(repeat('{"a":', 100), '1', repeat('}', 100))), 'a'))
SETTINGS allow_simdjson = 0, max_block_size = 65536, max_threads = 1, max_memory_usage = 150000000;
