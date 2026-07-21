-- keyValuePairs text index: m['key'] accessor must probe the index with the original key bytes even
-- when the key contains bytes that a subcolumn name carries through serializeText (newline, tab, NUL,
-- backslash, quote). Under optimize_functions_to_subcolumns = 1 the accessor is rewritten to the
-- subcolumn m.key_<serializeText(key)>; the index helper deserializes that suffix back through the map
-- key type (serializeText is the identity for String, but this locks in the contract). The index answer
-- must match a plain scan on every variant.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;

CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_mem VALUES
    (1, map(unhex('0A'), 'v')),      -- newline byte key
    (2, map(unhex('09'), 'v')),      -- tab byte key
    (3, map(unhex('00'), 'v')),      -- NUL byte key
    (4, map('a\\b', 'v')),           -- backslash key
    (5, map('q"x', 'v')),            -- double-quote key
    (6, map('plain', 'v'));          -- ordinary key
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- m[key] = value over special-byte keys: index == plain scan (subcolumn path) --';
SELECT id FROM t_mem WHERE m[unhex('0A')] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m[unhex('0A')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m[unhex('09')] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m[unhex('09')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m[unhex('00')] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m[unhex('00')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m['a\\b'] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m['a\\b'] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m['q"x'] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m['q"x'] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- startsWith over a special-byte key (subcolumn path) --';
SELECT id FROM t_mem WHERE startsWith(m[unhex('0A')], 'v') ORDER BY id;
SELECT id FROM t_idx WHERE startsWith(m[unhex('0A')], 'v') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- an absent special-byte key matches nothing --';
SELECT count() FROM t_idx WHERE m[unhex('0B')] = 'v' SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_mem;
DROP TABLE t_idx;
