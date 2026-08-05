-- Tests the filter-only postprocessor fast path for stop-word filters spelled as
-- if(token IN (...), '', token), NOT IN, an array right-hand side, and the equivalent
-- multiIf / CASE form. All spellings must drop the same tokens during index build. Also
-- covers non-plain-String index columns: a FixedString column and a MATERIALIZED String column.

SELECT 'if(token IN (tuple)): listed stop words dropped';

CREATE TABLE tab_in (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s IN ('the', 'a', 'is'), '', s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_in VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_in, idx);
SELECT count() FROM tab_in WHERE hasToken(s, 'fox');
SELECT count() FROM tab_in WHERE hasToken(s, 'end');

SELECT 'if(token NOT IN (keep-set)): only the keep-set survives';
CREATE TABLE tab_notin (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s NOT IN ('the', 'fox'), '', s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_notin VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_notin, idx);

SELECT 'if(token IN (array)): array right-hand side, same as tuple';
CREATE TABLE tab_array (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s IN ['the', 'a', 'is'], '', s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_array VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_array, idx);

SELECT 'multiIf(token IN (tuple)): CASE spelling drops the same tokens';
CREATE TABLE tab_multiif (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = multiIf(s IN ('the', 'a', 'is'), '', s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_multiif VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_multiif, idx);

SELECT 'FixedString index column: IN-filter drops the same stop words';
CREATE TABLE tab_fixed (
  id UInt32,
  s FixedString(20),
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s IN ('the', 'a', 'is'), '', s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_fixed VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

-- hasToken rejects a FixedString haystack, so verify the drop via the index dictionary.
SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_fixed, idx);

SELECT 'MATERIALIZED String index column: value computed, then IN-filter drops stop words';
CREATE TABLE tab_materialized (
  id UInt32,
  raw String,
  s String MATERIALIZED lower(raw),
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s IN ('the', 'a', 'is'), '', s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_materialized (id, raw) VALUES (1, 'The Quick Brown Fox'), (2, 'A Fox Is Here'), (3, 'Is The End');

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_materialized, idx);
SELECT count() FROM tab_materialized WHERE hasToken(s, 'fox');
SELECT count() FROM tab_materialized WHERE hasToken(s, 'the');

DROP TABLE tab_in;
DROP TABLE tab_notin;
DROP TABLE tab_array;
DROP TABLE tab_multiif;
DROP TABLE tab_fixed;
DROP TABLE tab_materialized;
