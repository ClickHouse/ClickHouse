-- A text index `preprocessor`/`postprocessor` must be authorized against the user declaring it.
-- `allow_introspection_functions` and the per-function grant share the gate the bug skipped, so the
-- setting stands in for a second user.

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

SELECT '3. Denied in a postprocessor.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '4. Denied when nested under a String result.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, demangle(val)))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '5. Denied via ALTER ADD INDEX.';

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)); -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab;

SELECT '6. Unprivileged expression unaffected.';

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

SELECT '7. Accepted with the privilege, so not a blocklist.';

SET allow_introspection_functions = 1;

-- `demangle` returns a non-mangled argument unchanged.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world');
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

SELECT '8. Existing index stays readable without the privilege.';

SET allow_introspection_functions = 0;

SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

SELECT '9. Unrelated ALTER does not re-authorize.';

ALTER TABLE tab ADD COLUMN extra UInt8 DEFAULT 0;
ALTER TABLE tab RENAME COLUMN val TO val2;
SELECT count() FROM tab WHERE hasAllTokens(val2, ['hello']);

SELECT '10. Neither does DETACH/ATTACH.';

DETACH TABLE tab;
ATTACH TABLE tab;
SELECT count() FROM tab WHERE hasAllTokens(val2, ['hello']);

SELECT '11. Redeclaring with a privileged function is denied.';

ALTER TABLE tab DROP INDEX idx,
                ADD INDEX idx(val2) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val2)) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab;
