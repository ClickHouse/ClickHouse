-- Positions of rare tokens are embedded into the dictionary block, so their phrases read nothing from `.pos`.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_ref;
DROP TABLE IF EXISTS tab_idx;

CREATE TABLE tab_ref
(
    id UInt32,
    message String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_idx
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

-- Rare tokens `alpha`, `bravo`, `charlie` among common ones; `zulu` is rare but has too many positions to embed.
INSERT INTO tab_ref SELECT number, multiIf(
    number = 10, 'w1 alpha bravo w2',
    number = 20, 'bravo alpha w3',
    number = 30, 'alpha w1 bravo charlie',
    number = 40, arrayStringConcat(arrayMap(i -> if(i % 37 = 0, 'zulu', concat('w', toString(i % 5))), range(5000)), ' '),
    concat('w', toString(number % 5), ' w', toString(number % 7)))
FROM numbers(500);
INSERT INTO tab_ref SELECT 500 + number, if(number = 7, 'charlie alpha bravo', concat('w', toString(number % 5), ' w', toString(number % 7)))
FROM numbers(500);
INSERT INTO tab_idx SELECT * FROM tab_ref;

SELECT 'Before merge';

SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE hasPhrase(message, 'alpha bravo') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_idx WHERE hasPhrase(message, 'alpha bravo') ORDER BY id);

SELECT count() FROM tab_ref WHERE hasPhrase(message, 'zulu w1');
SELECT count() FROM tab_idx WHERE hasPhrase(message, 'zulu w1');

OPTIMIZE TABLE tab_ref FINAL;
OPTIMIZE TABLE tab_idx FINAL;

SELECT 'After merge';

SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE hasPhrase(message, 'alpha bravo') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_idx WHERE hasPhrase(message, 'alpha bravo') ORDER BY id) SETTINGS log_comment = '05367_embedded';

SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE hasPhrase(message, 'bravo charlie') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_idx WHERE hasPhrase(message, 'bravo charlie') ORDER BY id);

SELECT count() FROM tab_ref WHERE hasPhrase(message, 'zulu w1');
SELECT count() FROM tab_idx WHERE hasPhrase(message, 'zulu w1') SETTINGS log_comment = '05367_in_pos';

SYSTEM FLUSH LOGS query_log;

SELECT 'Positions read from .pos';

SELECT log_comment, ProfileEvents['TextIndexPositionsBlocksRead'] > 0, ProfileEvents['TextIndexPositionsBytesRead'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05367_embedded', '05367_in_pos')
ORDER BY log_comment;

DROP TABLE tab_ref;
DROP TABLE tab_idx;
