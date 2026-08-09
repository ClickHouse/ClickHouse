-- Text-index `preprocessor` / `postprocessor` arguments referencing an ALIAS whose body
-- contains a column matcher must expand the matcher, not pass raw `COLUMNS(...)` downstream.

DROP TABLE IF EXISTS tab;

SELECT '-- Preprocessor referencing an ALIAS with a single-column matcher';

CREATE TABLE tab
(
    id UInt64,
    msg String,
    prep String ALIAS lower(COLUMNS('^msg$')),
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = prep)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab(id, msg) VALUES (1, 'Hello World'), (2, 'FOO bar');

SELECT count() FROM tab WHERE hasToken(msg, 'hello');
SELECT count() FROM tab WHERE hasToken(msg, 'HELLO'); -- search term is preprocessed too
SELECT count() FROM tab WHERE hasToken(msg, 'foo');
SELECT count() FROM tab WHERE hasToken(msg, 'missing');

-- The index can be rebuilt from persisted metadata.
ALTER TABLE tab MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT count() FROM tab WHERE hasToken(msg, 'world');

DETACH TABLE tab;
ATTACH TABLE tab;
SELECT count() FROM tab WHERE hasToken(msg, 'bar');

DROP TABLE tab;

SELECT '-- Postprocessor referencing an ALIAS with a matcher';

CREATE TABLE tab
(
    id UInt64,
    msg String,
    post String ALIAS upper(COLUMNS('^msg$')),
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = post)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab(id, msg) VALUES (1, 'hello world'), (2, 'foo bar');

SELECT count() FROM tab WHERE hasToken(msg, 'HELLO');
SELECT count() FROM tab WHERE hasToken(msg, 'FOO');
SELECT count() FROM tab WHERE hasToken(msg, 'missing');

DROP TABLE tab;

SELECT '-- Preprocessor referencing a chained ALIAS with matchers';

CREATE TABLE tab
(
    id UInt64,
    msg String,
    a String ALIAS lower(COLUMNS('^msg$')),
    b String ALIAS concat(COLUMNS('^a$'), ''),
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = b)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab(id, msg) VALUES (1, 'Hello World');

SELECT count() FROM tab WHERE hasToken(msg, 'hello');
SELECT count() FROM tab WHERE hasToken(msg, 'missing');

DROP TABLE tab;
