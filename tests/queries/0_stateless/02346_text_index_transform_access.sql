-- Text index `preprocessor` expressions must only use functions the user declaring
-- the index is permitted to run.
--
-- The test needs a function the current user may not run. Introspection functions fit: they are
-- refused both without the grant and, as here, with `allow_introspection_functions = 0`. Using the
-- setting keeps the test in plain SQL, with no second user to create.

DROP TABLE IF EXISTS tab;

-- The test configuration sets this to 1.
SET allow_introspection_functions = 0;

SELECT '1. Direct call denied.';

SELECT demangle('_ZNK2DB7Context9getAccessEv'); -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '2. Denied in a preprocessor.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '3. Denied when nested under a String result.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, demangle(val)))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '4. Denied via ALTER ADD INDEX.';

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)); -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab;

SELECT '5. Unprivileged expression unaffected.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello World');
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

DROP TABLE tab;

SELECT '6. Accepted with the privilege, so not a blocklist.';

SET allow_introspection_functions = 1;

-- `demangle` returns a non-mangled argument unchanged.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world');
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

SELECT '7. Existing index stays readable without the privilege.';

SET allow_introspection_functions = 0;

SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

SELECT '8. Unrelated ALTER does not re-authorize.';

ALTER TABLE tab ADD COLUMN extra UInt8 DEFAULT 0;
ALTER TABLE tab RENAME COLUMN val TO val2;
SELECT count() FROM tab WHERE hasAllTokens(val2, ['hello']);

SELECT '9. Neither does DETACH/ATTACH.';

DETACH TABLE tab;
ATTACH TABLE tab;
SELECT count() FROM tab WHERE hasAllTokens(val2, ['hello']);

SELECT '10. Redeclaring with a privileged function is denied.';

ALTER TABLE tab DROP INDEX idx,
                ADD INDEX idx(val2) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val2)) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab;

SELECT '11. The transform preflight does not reject what a plain ALTER accepts.';

CREATE TABLE tab_preflight (id UInt64, val String, num UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'minmax';

-- The preflight must drop implicit statistics too, or `ADD STATISTICS` looks like a duplicate.
ALTER TABLE tab_preflight ADD INDEX idx_pre(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val)) GRANULARITY 1,
                          ADD STATISTICS num TYPE minmax;

SELECT name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'tab_preflight' ORDER BY name;

DROP TABLE tab_preflight;

SELECT '12. A statement without a transform is not analyzed at all.';

SET allow_statistics = 1;
CREATE TABLE tab_plain (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS auto_statistics_types = 'minmax';

ALTER TABLE tab_plain ADD INDEX idx_minmax(x) TYPE minmax GRANULARITY 1, ADD STATISTICS x TYPE minmax;

SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_plain';

DROP TABLE tab_plain;
