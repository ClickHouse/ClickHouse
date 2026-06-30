-- Verify the lazy posting list apply mode (`text_index_posting_list_apply_mode = 'lazy'`) no longer
-- requires the experimental flag `allow_experimental_text_index_lazy_apply`, and that the now-obsolete
-- flag is still accepted (as a no-op) for backward compatibility.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_lazy_ga;

CREATE TABLE tab_lazy_ga(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_lazy_ga SELECT number, if(number % 2 = 0, 'common', 'uncommon') FROM numbers(2000);

-- Lazy mode without the experimental flag: must succeed and return correct results.
SELECT count() FROM tab_lazy_ga WHERE hasToken(s, 'common') SETTINGS text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_lazy_ga WHERE hasToken(s, 'common') AND hasToken(s, 'uncommon') SETTINGS text_index_posting_list_apply_mode = 'lazy';

-- The obsolete flag must still parse and be a no-op, whether set to 1 ...
SET allow_experimental_text_index_lazy_apply = 1;
SELECT count() FROM tab_lazy_ga WHERE hasToken(s, 'common') SETTINGS text_index_posting_list_apply_mode = 'lazy';

-- ... or 0 (it is no longer consulted).
SET allow_experimental_text_index_lazy_apply = 0;
SELECT count() FROM tab_lazy_ga WHERE hasToken(s, 'common') SETTINGS text_index_posting_list_apply_mode = 'lazy';

-- The setting is obsolete: present in system.settings and flagged as obsolete.
SELECT name, is_obsolete FROM system.settings WHERE name = 'allow_experimental_text_index_lazy_apply';

DROP TABLE tab_lazy_ga;
