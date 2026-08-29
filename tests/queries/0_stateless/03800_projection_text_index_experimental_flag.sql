-- Tests that CREATE TABLE / ALTER ADD PROJECTION respect both 'enable_full_text_index'
-- and 'allow_experimental_projection_text_index'. Projection-based text indexes are
-- gated independently because their on-disk format and reader path differ from the
-- GA skip text index.

DROP TABLE IF EXISTS tab;

-- Test CREATE TABLE

-- Either gate alone is not enough.

SET enable_full_text_index = 0;
SET allow_experimental_projection_text_index = 1;
CREATE TABLE tab1 (id UInt32, str String, PROJECTION idx INDEX str TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

SET enable_full_text_index = 1;
SET allow_experimental_projection_text_index = 0;
CREATE TABLE tab1 (id UInt32, str String, PROJECTION idx INDEX str TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

-- With both enabled CREATE TABLE succeeds.

SET enable_full_text_index = 1;
SET allow_experimental_projection_text_index = 1;
CREATE TABLE tab1 (id UInt32, str String, PROJECTION idx INDEX str TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab1;

-- Test ADD PROJECTION

SET enable_full_text_index = 0;
SET allow_experimental_projection_text_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD PROJECTION idx1 INDEX str TYPE text(tokenizer = 'splitByNonAlpha');  -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tab;

SET enable_full_text_index = 1;
SET allow_experimental_projection_text_index = 0;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD PROJECTION idx1 INDEX str TYPE text(tokenizer = 'splitByNonAlpha');  -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tab;

SET enable_full_text_index = 1;
SET allow_experimental_projection_text_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD PROJECTION idx1 INDEX str TYPE text(tokenizer = 'splitByNonAlpha');
DROP TABLE tab;
