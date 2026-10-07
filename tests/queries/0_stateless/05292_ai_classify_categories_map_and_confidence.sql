-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- `aiClassify` with `Map` categories, `return_confidence`, and `provider = 'typesafe'`. No network requests are made.

DROP NAMED COLLECTION IF EXISTS ai_05292_typesafe;
CREATE NAMED COLLECTION ai_05292_typesafe AS
    provider = 'typesafe',
    endpoint = 'http://localhost:1/v1/systemone',
    model = 'jev-latest',
    api_key = 'fake-key';

DROP NAMED COLLECTION IF EXISTS ai_05292_openai;
CREATE NAMED COLLECTION ai_05292_openai AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x String, n Nullable(String)) ENGINE = Memory;

-- The function runs on the empty header block, so every call resolves the credentials.
SELECT '-- Return types';
DESCRIBE (SELECT aiClassify(x, map('a', 'A', 'b', 'B'), map('credentials', 'ai_05292_typesafe')) FROM tab);
DESCRIBE (SELECT aiClassify(n, map('a', 'A', 'b', 'B'), map('credentials', 'ai_05292_typesafe')) FROM tab);
DESCRIBE (SELECT aiClassify(x, ['a', 'b'], map('credentials', 'ai_05292_typesafe', 'return_confidence', '1')) FROM tab);
DESCRIBE (SELECT aiClassify(n, map('a', 'A'), map('credentials', 'ai_05292_typesafe', 'return_confidence', '1')) FROM tab);
DESCRIBE (SELECT aiClassify(x, ['a', 'b'], map('credentials', 'ai_05292_typesafe', 'return_confidence', '0')) FROM tab);
SELECT aiClassify(x, ['a'], map('credentials', 'ai_05292_typesafe', 'return_confidence', 'yes')) FROM tab; -- { serverError BAD_ARGUMENTS }

SELECT '-- Map categories without a parameter map';
SET ai_function_text_default_credentials = 'ai_05292_typesafe';
DESCRIBE (SELECT aiClassify(x, map('a', 'A', 'b', 'B')) FROM tab);
SET ai_function_text_default_credentials = '';

SELECT '-- Categories validation';
SELECT aiClassify(x, map()::Map(String, String), map('credentials', 'ai_05292_typesafe')) FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT aiClassify(x, map('a', 'A', 'a', 'B'), map('credentials', 'ai_05292_typesafe')) FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT aiClassify(x, map(x, x), map('credentials', 'ai_05292_typesafe')) FROM tab; -- { serverError ILLEGAL_COLUMN }
SELECT aiClassify(x, map('a', 1), map('credentials', 'ai_05292_typesafe')) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- typesafe provider';
SELECT aiClassify(x, ['a', 'b'], map('credentials', 'ai_05292_typesafe')) FROM tab;
SELECT aiClassify(x, map('a', 'A'), map('credentials', 'ai_05292_typesafe', 'return_confidence', '1')) FROM tab;
SELECT aiGenerate(x, map('credentials', 'ai_05292_typesafe')) FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT aiFilter(x, 'is a bug', map('credentials', 'ai_05292_typesafe')) FROM tab; -- { serverError BAD_ARGUMENTS }

SELECT '-- return_confidence requires typesafe';
SELECT aiClassify(x, ['a', 'b'], map('credentials', 'ai_05292_openai', 'return_confidence', '1')) FROM tab; -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;
DROP NAMED COLLECTION ai_05292_typesafe;
DROP NAMED COLLECTION ai_05292_openai;
