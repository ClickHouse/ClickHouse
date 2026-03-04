-- Tags: no-fasttest
-- no-fasttest: It can be slow

-- Equivalence test: v1 and v2 posting list formats must produce identical query results.
-- Two tables with the same schema (only posting_list_version differs) receive the same data.
-- Every query is run against both tables and the results must match.
-- Covers hasToken, hasAnyTokens (array + string), hasAllTokens (array + string).

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS tab_eq_v1;
DROP TABLE IF EXISTS tab_eq_v2;

----------------------------------------------------
-- Schema: identical except posting_list_version
----------------------------------------------------

CREATE TABLE tab_eq_v1(
    k UInt64,
    s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_version = 1)
) ENGINE = MergeTree() ORDER BY k
  SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE tab_eq_v2(
    k UInt64,
    s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_version = 2)
) ENGINE = MergeTree() ORDER BY k
  SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

----------------------------------------------------
-- Part 1: Small data
----------------------------------------------------
SELECT 'Part 1: Small data';

INSERT INTO tab_eq_v1 SELECT number, if(number % 3 = 0, 'apple banana', if(number % 3 = 1, 'cherry date', 'elderberry fig')) FROM numbers(300);
INSERT INTO tab_eq_v2 SELECT number, if(number % 3 = 0, 'apple banana', if(number % 3 = 1, 'cherry date', 'elderberry fig')) FROM numbers(300);

-- hasToken: single token queries
SELECT 'hasToken apple';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'apple');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'apple');

SELECT 'hasToken cherry';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'cherry');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'cherry');

SELECT 'hasToken elderberry';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'elderberry');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'elderberry');

-- hasToken: AND (intersection) — tokens from different groups, expect 0
SELECT 'AND intersection (empty)';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'apple') AND hasToken(s, 'cherry');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'apple') AND hasToken(s, 'cherry');

-- hasToken: AND — tokens from the same group, expect 100
SELECT 'AND same group';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'apple') AND hasToken(s, 'banana');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'apple') AND hasToken(s, 'banana');

-- hasToken: OR (union)
SELECT 'OR union';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'apple') OR hasToken(s, 'cherry');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'apple') OR hasToken(s, 'cherry');

-- hasToken: non-existent token
SELECT 'non-existent token';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'grape');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'grape');

-- hasToken: row values
SELECT 'row values';
SELECT k, s FROM tab_eq_v1 WHERE hasToken(s, 'apple') ORDER BY k LIMIT 5;
SELECT k, s FROM tab_eq_v2 WHERE hasToken(s, 'apple') ORDER BY k LIMIT 5;

-- hasAnyTokens: array form — match any of the listed tokens
SELECT 'hasAnyTokens array: apple or cherry';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['apple', 'cherry']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['apple', 'cherry']);

SELECT 'hasAnyTokens array: elderberry or grape';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['elderberry', 'grape']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['elderberry', 'grape']);

SELECT 'hasAnyTokens array: all three';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['apple', 'cherry', 'elderberry']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['apple', 'cherry', 'elderberry']);

SELECT 'hasAnyTokens array: none exist';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['grape', 'mango']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['grape', 'mango']);

-- hasAnyTokens: string form
SELECT 'hasAnyTokens string: apple cherry';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, 'apple cherry');
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, 'apple cherry');

SELECT 'hasAnyTokens string: grape mango';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, 'grape mango');
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, 'grape mango');

-- hasAllTokens: array form — match all listed tokens
SELECT 'hasAllTokens array: apple and banana';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['apple', 'banana']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['apple', 'banana']);

SELECT 'hasAllTokens array: apple and cherry (empty)';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['apple', 'cherry']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['apple', 'cherry']);

SELECT 'hasAllTokens array: cherry and date';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['cherry', 'date']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['cherry', 'date']);

SELECT 'hasAllTokens array: single token apple';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['apple']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['apple']);

SELECT 'hasAllTokens array: non-existent grape';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['apple', 'grape']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['apple', 'grape']);

-- hasAllTokens: string form
SELECT 'hasAllTokens string: apple banana';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'apple banana');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'apple banana');

SELECT 'hasAllTokens string: apple cherry';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'apple cherry');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'apple cherry');

----------------------------------------------------
-- Part 2: Large data (triggers large posting list path)
----------------------------------------------------
SELECT 'Part 2: Large data (triggers large posting list path)';

TRUNCATE TABLE tab_eq_v1;
TRUNCATE TABLE tab_eq_v2;

INSERT INTO tab_eq_v1 SELECT number, if(number % 5 = 0, 'common frequent', if(number % 5 = 1, 'common medium', if(number % 5 = 2, 'rare special', if(number % 5 = 3, 'unique odd', 'unique even')))) FROM numbers(5000);
INSERT INTO tab_eq_v2 SELECT number, if(number % 5 = 0, 'common frequent', if(number % 5 = 1, 'common medium', if(number % 5 = 2, 'rare special', if(number % 5 = 3, 'unique odd', 'unique even')))) FROM numbers(5000);

-- hasToken
SELECT 'large: hasToken common';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'common');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'common');

SELECT 'large: hasToken rare';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'rare');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'rare');

SELECT 'large: hasToken frequent';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'frequent');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'frequent');

SELECT 'large: AND common AND frequent';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'common') AND hasToken(s, 'frequent');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'common') AND hasToken(s, 'frequent');

SELECT 'large: AND common AND rare (empty)';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'common') AND hasToken(s, 'rare');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

SELECT 'large: OR rare OR unique';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'rare') OR hasToken(s, 'unique');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'rare') OR hasToken(s, 'unique');

SELECT 'large: 3-way AND common AND medium (not frequent)';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'common') AND hasToken(s, 'medium');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'common') AND hasToken(s, 'medium');

-- hasAnyTokens: array form
SELECT 'large: hasAnyTokens array: common or rare';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['common', 'rare']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['common', 'rare']);

SELECT 'large: hasAnyTokens array: frequent or special or odd';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['frequent', 'special', 'odd']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['frequent', 'special', 'odd']);

SELECT 'large: hasAnyTokens array: nonexistent';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['nonexistent', 'missing']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['nonexistent', 'missing']);

SELECT 'large: hasAnyTokens array: one exists one not';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['common', 'nonexistent']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['common', 'nonexistent']);

-- hasAnyTokens: string form
SELECT 'large: hasAnyTokens string: common rare';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, 'common rare');
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, 'common rare');

SELECT 'large: hasAnyTokens string: frequent special odd';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, 'frequent special odd');
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, 'frequent special odd');

-- hasAllTokens: array form
SELECT 'large: hasAllTokens array: common and frequent';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['common', 'frequent']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['common', 'frequent']);

SELECT 'large: hasAllTokens array: common and medium';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['common', 'medium']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['common', 'medium']);

SELECT 'large: hasAllTokens array: common and rare (empty)';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['common', 'rare']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['common', 'rare']);

SELECT 'large: hasAllTokens array: rare and special';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['rare', 'special']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['rare', 'special']);

SELECT 'large: hasAllTokens array: unique and even';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['unique', 'even']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['unique', 'even']);

SELECT 'large: hasAllTokens array: common and nonexistent';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['common', 'nonexistent']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['common', 'nonexistent']);

-- hasAllTokens: string form
SELECT 'large: hasAllTokens string: common frequent';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'common frequent');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'common frequent');

SELECT 'large: hasAllTokens string: rare special';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'rare special');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'rare special');

SELECT 'large: hasAllTokens string: common rare';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'common rare');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'common rare');

-- Mixed: hasAnyTokens combined with hasToken via AND/OR
SELECT 'large: hasAnyTokens AND hasToken';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['frequent', 'medium']) AND hasToken(s, 'common');
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['frequent', 'medium']) AND hasToken(s, 'common');

SELECT 'large: hasAllTokens OR hasToken';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['rare', 'special']) OR hasToken(s, 'even');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['rare', 'special']) OR hasToken(s, 'even');

----------------------------------------------------
-- Part 3: After merge
----------------------------------------------------
SELECT 'Part 3: After merge';

TRUNCATE TABLE tab_eq_v1;
TRUNCATE TABLE tab_eq_v2;

INSERT INTO tab_eq_v1 SELECT number, if(number % 2 = 0, 'merge alpha', 'merge beta') FROM numbers(2000);
INSERT INTO tab_eq_v1 SELECT number + 2000, if((number + 2000) % 2 = 0, 'merge alpha', 'merge beta') FROM numbers(2000);

INSERT INTO tab_eq_v2 SELECT number, if(number % 2 = 0, 'merge alpha', 'merge beta') FROM numbers(2000);
INSERT INTO tab_eq_v2 SELECT number + 2000, if((number + 2000) % 2 = 0, 'merge alpha', 'merge beta') FROM numbers(2000);

OPTIMIZE TABLE tab_eq_v1 FINAL;
OPTIMIZE TABLE tab_eq_v2 FINAL;

-- hasToken
SELECT 'after merge: hasToken alpha';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'alpha');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'alpha');

SELECT 'after merge: hasToken beta';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'beta');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'beta');

SELECT 'after merge: AND alpha AND beta (empty)';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'alpha') AND hasToken(s, 'beta');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'alpha') AND hasToken(s, 'beta');

SELECT 'after merge: AND merge AND alpha';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'merge') AND hasToken(s, 'alpha');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'merge') AND hasToken(s, 'alpha');

SELECT 'after merge: OR alpha OR beta';
SELECT count() FROM tab_eq_v1 WHERE hasToken(s, 'alpha') OR hasToken(s, 'beta');
SELECT count() FROM tab_eq_v2 WHERE hasToken(s, 'alpha') OR hasToken(s, 'beta');

-- hasAnyTokens: array form
SELECT 'after merge: hasAnyTokens array: alpha or beta';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['alpha', 'beta']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['alpha', 'beta']);

SELECT 'after merge: hasAnyTokens array: alpha or nonexistent';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['alpha', 'nonexistent']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['alpha', 'nonexistent']);

SELECT 'after merge: hasAnyTokens array: nonexistent only';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['nonexistent', 'missing']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['nonexistent', 'missing']);

-- hasAnyTokens: string form
SELECT 'after merge: hasAnyTokens string: alpha beta';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, 'alpha beta');
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, 'alpha beta');

-- hasAllTokens: array form
SELECT 'after merge: hasAllTokens array: merge and alpha';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['merge', 'alpha']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['merge', 'alpha']);

SELECT 'after merge: hasAllTokens array: merge and beta';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['merge', 'beta']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['merge', 'beta']);

SELECT 'after merge: hasAllTokens array: alpha and beta (empty)';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['alpha', 'beta']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['alpha', 'beta']);

SELECT 'after merge: hasAllTokens array: merge and nonexistent';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, ['merge', 'nonexistent']);
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, ['merge', 'nonexistent']);

-- hasAllTokens: string form
SELECT 'after merge: hasAllTokens string: merge alpha';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'merge alpha');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'merge alpha');

SELECT 'after merge: hasAllTokens string: alpha beta';
SELECT count() FROM tab_eq_v1 WHERE hasAllTokens(s, 'alpha beta');
SELECT count() FROM tab_eq_v2 WHERE hasAllTokens(s, 'alpha beta');

-- Mixed after merge
SELECT 'after merge: hasAnyTokens AND hasAllTokens';
SELECT count() FROM tab_eq_v1 WHERE hasAnyTokens(s, ['alpha', 'beta']) AND hasAllTokens(s, ['merge']);
SELECT count() FROM tab_eq_v2 WHERE hasAnyTokens(s, ['alpha', 'beta']) AND hasAllTokens(s, ['merge']);

----------------------------------------------------
-- Cleanup
----------------------------------------------------
DROP TABLE tab_eq_v1;
DROP TABLE tab_eq_v2;
