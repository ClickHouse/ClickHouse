-- Tests for CREATE/ALTER/DROP HANDLER: parsing, the registry, ambiguity checks,
-- type validation and the system.handlers introspection table.
-- Handler names/URLs are globally unique to this test to avoid cross-test interference.

DROP HANDLER IF EXISTS h04303_a;
DROP HANDLER IF EXISTS h04303_b;
DROP HANDLER IF EXISTS h04303_c;

-- Basic CREATE with an exact URL, default method (GET) and default type (query).
CREATE HANDLER h04303_a URL '/test_04303/a' AS SELECT 1 AS x;

SELECT name, protocol, url_match_type, url, methods, type, query
FROM system.handlers WHERE name = 'h04303_a';

-- CREATE IF NOT EXISTS must be a no-op when the handler already exists.
CREATE HANDLER IF NOT EXISTS h04303_a URL '/test_04303/other' AS SELECT 2;
SELECT url FROM system.handlers WHERE name = 'h04303_a';

-- Creating an existing handler without IF NOT EXISTS must fail.
CREATE HANDLER h04303_a URL '/test_04303/dup' AS SELECT 3; -- { serverError HANDLER_ALREADY_EXISTS }

-- PROTOCOL, URL PREFIX, METHODS and TYPE clauses.
CREATE HANDLER h04303_b PROTOCOL my_proto URL PREFIX '/test_04303/b/' METHODS (GET, POST) TYPE query AS SELECT 'b';
SELECT name, protocol, url_match_type, url, methods, type FROM system.handlers WHERE name = 'h04303_b';

-- URL REGEXP. The query is parsed but not analyzed: it may reference a non-existing table.
CREATE HANDLER h04303_c URL REGEXP '/test_04303/c/(?P<id>[0-9]+)' AS SELECT * FROM no_such_db.no_such_table;
SELECT name, url_match_type, url FROM system.handlers WHERE name = 'h04303_c';

-- Unsupported handler type must fail.
CREATE HANDLER h04303_bad URL '/test_04303/bad' TYPE static AS SELECT 1; -- { serverError BAD_ARGUMENTS }

-- Unknown HTTP method must fail to parse.
CREATE HANDLER h04303_bad URL '/test_04303/bad' METHODS (TRACE) AS SELECT 1; -- { clientError SYNTAX_ERROR }

-- Ambiguity: exact URL equal to an existing exact URL with overlapping method and protocol.
CREATE HANDLER h04303_dup_exact URL '/test_04303/a' AS SELECT 1; -- { serverError AMBIGUOUS_HANDLER }

-- Ambiguity: a prefix that covers an existing exact URL (same default GET method).
CREATE HANDLER h04303_dup_prefix URL PREFIX '/test_04303/' AS SELECT 1; -- { serverError AMBIGUOUS_HANDLER }

-- Not ambiguous: same URL but disjoint methods.
CREATE HANDLER h04303_post_a URL '/test_04303/a' METHODS (POST) AS SELECT 1;
SELECT name, methods FROM system.handlers WHERE name = 'h04303_post_a';
DROP HANDLER h04303_post_a;

-- ALTER: change only the query (other clauses are preserved).
ALTER HANDLER h04303_a AS SELECT 42 AS y;
SELECT url, methods, query FROM system.handlers WHERE name = 'h04303_a';

-- ALTER: change only the URL.
ALTER HANDLER h04303_a URL PREFIX '/test_04303/a_new';
SELECT url_match_type, url, query FROM system.handlers WHERE name = 'h04303_a';

-- ALTER: change methods.
ALTER HANDLER h04303_a METHODS (GET, POST, PUT, DELETE);
SELECT methods FROM system.handlers WHERE name = 'h04303_a';

-- ALTER a non-existing handler must fail.
ALTER HANDLER h04303_absent AS SELECT 1; -- { serverError HANDLER_DOESNT_EXIST }

-- DROP a non-existing handler without IF EXISTS must fail.
DROP HANDLER h04303_absent; -- { serverError HANDLER_DOESNT_EXIST }

-- DROP IF EXISTS on a non-existing handler is a no-op.
DROP HANDLER IF EXISTS h04303_absent;

-- Count of this test's handlers, then clean up.
SELECT count() FROM system.handlers WHERE name LIKE 'h04303\_%';

DROP HANDLER h04303_a;
DROP HANDLER h04303_b;
DROP HANDLER h04303_c;

SELECT count() FROM system.handlers WHERE name LIKE 'h04303\_%';
