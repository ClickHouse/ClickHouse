SET allow_experimental_projection_text_index = 1;

-- Verify that hasPhrase correctness is preserved through projection text index merge.
-- Regression test for position data ordering bug when merging projection parts
-- with mixed embedded (<=6 docs) and large (>6 docs) posting lists.

DROP TABLE IF EXISTS t_phrase_merge_bug;

CREATE TABLE t_phrase_merge_bug (id UInt64, body String,
    PROJECTION idx_text INDEX body TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        enable_phrase_query_support = 1))
ENGINE = MergeTree ORDER BY id;

-- Part 1: 22 rows. Token "United" appears in ~6 rows (= MAX_SIZE_OF_EMBEDDED_POSTINGS).
INSERT INTO t_phrase_merge_bug VALUES
    (0, '{{Redirect|Anarchist'), (200, ' between the [[front'), (400, ' Endzone.jpg|thumb|r'),
    (478, ' bar in the United S'), (509, ' United States#Suspe'), (563, ' United States|polit'),
    (571, ' United States was e'), (600, ' [[Category:Members '), (800, ' different branches,'),
    (921, ' United States|Ameri'), (1000, '{{Infobox person | n'), (1200, ' civilization.<ref>W'),
    (1400, '{{wiktionary|Austin}'), (1600, ' |title=Santali alph'), (1800, '{{About|the U.S. sta'),
    (1907, ' States topics}}  [['), (2000, ' [[mechanization]], '), (2200, ' [[carboxylic acid]]'),
    (2210, ' the United States=='), (2400, ' consumed in the [[U'), (2600, ' (1.57 to 1.93 m).  '),
    (2800, ' the idea of the [[a');

-- Part 2: 24 rows. Token "United" appears in ~8 rows (> MAX_SIZE_OF_EMBEDDED_POSTINGS).
INSERT INTO t_phrase_merge_bug VALUES
    (3000, ' of people. Some are'), (3054, ' [[United States Con'), (3200, '         = {{increas'),
    (3290, ' 17.39%, [[United St'), (3333, ' |publisher=United N'), (3340, ' States|United State'),
    (3400, ' |+Average daily '), (3600, ' inventors]] [[Categ'), (3800, '{{About|the modern s'),
    (3979, ' United States]] [[B'), (4000, ' the management of o'), (4200, ' Selz]], eds., Theor'),
    (4228, ' States from 2001 to'), (4329, ' {{flagicon|United S'), (4348, ' United States as a '),
    (4400, ' and West Indies, un'), (4439, ' AC === Most United '), (4600, ' name=N31-M/><ref na'),
    (4800, ' name=Davis_Stevens'), (5000, ' into the Roman Empi'), (5200, ' Director|Best Direc'),
    (5400, ' the Romans turn ove'), (5600, ' Volkswagen Group as'), (5800, ' /><ref>[http://lcwe');

SELECT 'before_optimize_hasPhrase', count() FROM t_phrase_merge_bug WHERE hasPhrase(body, 'United States') SETTINGS max_threads = 1;
SELECT 'before_optimize_hasAllTokens', count() FROM t_phrase_merge_bug WHERE hasAllTokens(body, 'United States') SETTINGS max_threads = 1;

OPTIMIZE TABLE t_phrase_merge_bug FINAL;

SELECT sleepEachRow(0.1) FROM numbers(50)
WHERE (SELECT count() FROM system.parts WHERE active AND table = 't_phrase_merge_bug' AND database = currentDatabase()) > 1
FORMAT Null;

SELECT 'after_optimize_hasPhrase', count() FROM t_phrase_merge_bug WHERE hasPhrase(body, 'United States') SETTINGS max_threads = 1;
SELECT 'after_optimize_hasAllTokens', count() FROM t_phrase_merge_bug WHERE hasAllTokens(body, 'United States') SETTINGS max_threads = 1;

DROP TABLE t_phrase_merge_bug;
