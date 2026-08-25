-- An over-complex regexp pattern (here a large LIKE / match pattern) must
-- report a normal error instead of aborting in debug builds.
SELECT 'x' LIKE repeat('a%', 600000); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('x', repeat('.*a', 400000)); -- { serverError CANNOT_COMPILE_REGEXP }
