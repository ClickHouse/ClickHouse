-- Tags: no-fasttest
-- Test the keep-on-match spelling of the filter-only text-index postprocessor:
-- if(token IN/NOT IN (...), token, '') keeps the token on the true branch and drops it on the false branch,
-- the inverse of the drop-on-match if(cond, '', token) form covered by the PR's own test.

DROP TABLE IF EXISTS tab_keep_in;
CREATE TABLE tab_keep_in (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s IN ('the', 'fox'), s, ''))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_keep_in VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

SELECT 'if(token IN (...), token, ...): only the listed tokens survive';
SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_keep_in, idx);
SELECT count() FROM tab_keep_in WHERE hasToken(s, 'fox');
SELECT count() FROM tab_keep_in WHERE hasToken(s, 'brown');

SELECT 'if(token NOT IN (...), token, ...): keep-on-match combined with NOT IN inversion';
DROP TABLE IF EXISTS tab_keep_notin;
CREATE TABLE tab_keep_notin (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s NOT IN ('the', 'fox'), s, ''))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_keep_notin VALUES (1, 'the quick brown fox'), (2, 'a fox is here'), (3, 'is the end');

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_keep_notin, idx);
SELECT count() FROM tab_keep_notin WHERE hasToken(s, 'fox');
SELECT count() FROM tab_keep_notin WHERE hasToken(s, 'brown');

DROP TABLE tab_keep_in;
DROP TABLE tab_keep_notin;
