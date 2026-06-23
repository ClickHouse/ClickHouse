-- The LAZY_LOAD clause of CREATE DICTIONARY must parse and round-trip through the formatter.

SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, val String) PRIMARY KEY id SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0) LAZY_LOAD(1)$$);
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, val String) PRIMARY KEY id SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0) LAZY_LOAD(0)$$);
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, val String) PRIMARY KEY id SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0)$$);

-- A duplicate LAZY_LOAD clause is a syntax error, like any other repeated dictionary clause.
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0) LAZY_LOAD(1) LAZY_LOAD(0)$$); -- { serverError SYNTAX_ERROR }
