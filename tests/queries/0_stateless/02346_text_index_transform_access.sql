-- A text index `preprocessor`/`postprocessor` expression must be authorized against the user who
-- submits the DDL, not resolved under the global full-access context.
--
-- `allow_introspection_functions` and the per-function grant are enforced side by side in
-- `ContextAccess::checkAccessImplHelper`, past the `full_access` short-circuit that used to be taken,
-- so the setting reaches the same gate without needing a second user.

DROP TABLE IF EXISTS tab;

-- Not the default in the test configuration, see tests/config/users.d/allow_introspection_functions.yaml.
SET allow_introspection_functions = 0;

SELECT '1. The boundary: a direct call is denied.';

SELECT demangle('_ZNK2DB7Context9getAccessEv'); -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '2. The same function in a preprocessor is denied too.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '3. And in a postprocessor.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '4. A privileged call nested below a String-typed top level is denied.';

-- The result type gate only constrains the top of the expression, so it bounds nothing on its own.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, demangle(val)))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '5. ALTER ... ADD INDEX is denied on the same grounds.';

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)); -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab;

SELECT '6. An unprivileged expression is unaffected.';

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

SELECT '7. With the privilege the same expression is accepted: this is authorization, not a blocklist.';

SET allow_introspection_functions = 1;

-- `demangle` returns a non-mangled argument unchanged, so the tokens here are just the words.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world');
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

SELECT '8. An existing index stays readable without the privilege: it is authorized when defined, not per reader.';

SET allow_introspection_functions = 0;

SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

SELECT '9. An unrelated ALTER on such a table does not re-authorize the index.';

ALTER TABLE tab ADD COLUMN extra UInt8 DEFAULT 0;
ALTER TABLE tab RENAME COLUMN val TO val2;
SELECT count() FROM tab WHERE hasAllTokens(val2, ['hello']);

SELECT '10. Nor does detaching and attaching the stored definition.';

DETACH TABLE tab;
ATTACH TABLE tab;
SELECT count() FROM tab WHERE hasAllTokens(val2, ['hello']);

SELECT '11. Redeclaring the index with a privileged function is still denied.';

ALTER TABLE tab DROP INDEX idx,
                ADD INDEX idx(val2) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val2)) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab;
