-- The pattern of `GRANT READ ON S3('...')` is not compiled by the parser - that would put a regex
-- engine in it - so every path that turns the query into access rights validates it instead. A
-- pattern that does not compile is matched with `RE2::FullMatch`, so it would grant nothing while
-- looking accepted.

DROP USER IF EXISTS user_04661;
CREATE USER user_04661;

GRANT READ ON S3('[') TO user_04661; -- { serverError CANNOT_COMPILE_REGEXP }

-- `CHECK GRANT` builds the same elements through its own interpreter.
CHECK GRANT READ ON S3('['); -- { serverError CANNOT_COMPILE_REGEXP }

-- A valid pattern is accepted on both paths.
GRANT READ ON S3('s3://bucket/.*') TO user_04661;
CHECK GRANT READ ON S3('s3://bucket/.*');

DROP USER user_04661;
