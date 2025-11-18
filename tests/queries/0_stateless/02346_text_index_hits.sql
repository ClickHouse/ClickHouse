-- Tags: stateful, long, no-parallel, no-asan, no-tsan, no-ubsan, no-msan
-- no-*san: too long.

DROP TABLE IF EXISTS hits_text;

CREATE TABLE hits_text
(
    `CounterID` UInt32,
    `EventDate` Date,
    `UserID` UInt32,
    `SearchPhrase` String,
    `URL` String
)
ENGINE = MergeTree
ORDER BY (CounterID, EventDate);

SET allow_experimental_full_text_index = 1;
SET use_query_condition_cache = 0;

ALTER TABLE hits_text ADD INDEX idx_search_phrase SearchPhrase TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 8;
ALTER TABLE hits_text ADD INDEX idx_url URL TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 8;

SET max_insert_threads = 4;
INSERT INTO hits_text SELECT CounterID, EventDate, UserID,SearchPhrase, URL FROM test.hits;

SELECT 'hasToken reference without index';

SET use_skip_indexes = 0;
SET use_skip_indexes_on_data_read = 0;
SET force_data_skipping_indices = '';

SELECT 'idx_search_phrase';

SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'video');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'google');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'market');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'world');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'mail');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'amazon');
SELECT uniqExact(UserID) FROM hits_text WHERE hasToken(SearchPhrase, 'anime');

SELECT 'idx_url';

SELECT count() FROM hits_text WHERE hasToken(URL, 'com');
SELECT count() FROM hits_text WHERE hasToken(URL, 'com') AND hasToken(URL, 'mail');
SELECT count() FROM hits_text WHERE hasToken(URL, 'com') AND NOT hasToken(URL, 'mail');
SELECT count() FROM hits_text WHERE hasToken(URL, 'http');
SELECT count() FROM hits_text WHERE hasToken(URL, 'https');
SELECT count() FROM hits_text WHERE hasToken(URL, 'https') AND CounterID = 33290414;
SELECT count() FROM hits_text WHERE (hasToken(URL, 'https') OR UserID = 7541501) AND CounterID = 33290414;

SELECT 'idx_search_phrase,idx_url';

SELECT uniqExact(UserID), min(EventDate), max(EventDate) FROM hits_text WHERE hasToken(URL, 'https') AND hasToken(SearchPhrase, 'video');
SELECT count() FROM hits_text WHERE hasToken(URL, 'auto') AND hasToken(SearchPhrase, 'bmw');

SELECT 'hasToken direct read from index';

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

SELECT 'idx_search_phrase';
SET force_data_skipping_indices = 'idx_search_phrase';

SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'video');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'google');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'market');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'world');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'mail');
SELECT count() FROM hits_text WHERE hasToken(SearchPhrase, 'amazon');
SELECT uniqExact(UserID) FROM hits_text WHERE hasToken(SearchPhrase, 'anime');

SELECT 'idx_url';
SET force_data_skipping_indices = 'idx_url';

SELECT count() FROM hits_text WHERE hasToken(URL, 'com');
SELECT count() FROM hits_text WHERE hasToken(URL, 'com') AND hasToken(URL, 'mail');
SELECT count() FROM hits_text WHERE hasToken(URL, 'com') AND NOT hasToken(URL, 'mail');
SELECT count() FROM hits_text WHERE hasToken(URL, 'http');
SELECT count() FROM hits_text WHERE hasToken(URL, 'https');
SELECT count() FROM hits_text WHERE hasToken(URL, 'https') AND CounterID = 33290414;
SELECT count() FROM hits_text WHERE (hasToken(URL, 'https') OR UserID = 7541501) AND CounterID = 33290414;

SELECT 'idx_search_phrase,idx_url';
SET force_data_skipping_indices = 'idx_search_phrase,idx_url';

SELECT uniqExact(UserID), min(EventDate), max(EventDate) FROM hits_text WHERE hasToken(URL, 'https') AND hasToken(SearchPhrase, 'video');
SELECT count() FROM hits_text WHERE hasToken(URL, 'auto') AND hasToken(SearchPhrase, 'bmw');

SELECT 'hasAnyTokens/hasAllTokens reference without direct read from index';

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET force_data_skipping_indices = 'idx_url';

SELECT count() FROM hits_text WHERE hasAnyTokens(URL, ['https', 'http']);
SELECT count() FROM hits_text WHERE hasAllTokens(URL, ['com', 'mail']);
SELECT count() FROM hits_text WHERE hasAllTokens(URL, ['com', 'mail']) AND NOT hasToken(URL, 'http');
SELECT count() FROM hits_text WHERE hasAnyTokens(URL, ['facebook', 'twitter']);
SELECT count() FROM hits_text WHERE hasToken(URL, 'auto') AND hasAnyTokens(SearchPhrase, ['bmw', 'audi', 'toyota']);
SELECT count() FROM hits_text WHERE hasAnyTokens(URL, ['market', 'shop']) OR hasAnyTokens(SearchPhrase, ['market', 'shop']);

SELECT 'hasAnyTokens/hasAllTokens direct read from index';

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET force_data_skipping_indices = 'idx_url';

SELECT count() FROM hits_text WHERE hasAnyTokens(URL, ['https', 'http']);
SELECT count() FROM hits_text WHERE hasAllTokens(URL, ['com', 'mail']);
SELECT count() FROM hits_text WHERE hasAllTokens(URL, ['com', 'mail']) AND NOT hasToken(URL, 'http');
SELECT count() FROM hits_text WHERE hasAnyTokens(URL, ['facebook', 'twitter']);
SELECT count() FROM hits_text WHERE hasToken(URL, 'auto') AND hasAnyTokens(SearchPhrase, ['bmw', 'audi', 'toyota']);
SELECT count() FROM hits_text WHERE hasAnyTokens(URL, ['market', 'shop']) OR hasAnyTokens(SearchPhrase, ['market', 'shop']);

DROP TABLE hits_text;
