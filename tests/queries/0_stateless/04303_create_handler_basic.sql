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

-- URL is mandatory for CREATE.
CREATE HANDLER h04303_nourl AS SELECT 1; -- { clientError SYNTAX_ERROR }

-- The query is parsed for syntactic correctness at creation time.
CREATE HANDLER h04303_bad_query URL '/test_04303/badq' AS SELECT FROM WHERE; -- { clientError SYNTAX_ERROR }

-- The query can be put in parentheses for disambiguation; the parentheses are not stored.
CREATE HANDLER h04303_paren URL '/test_04303/paren' AS (SELECT 1);
SELECT query FROM system.handlers WHERE name = 'h04303_paren';
DROP HANDLER h04303_paren;

-- Arbitrary queries are supported, not only SELECT/INSERT.
CREATE HANDLER h04303_show URL '/test_04303/show' AS SHOW DATABASES;
SELECT query FROM system.handlers WHERE name = 'h04303_show';
DROP HANDLER h04303_show;

-- The FORMAT clause belongs to the query, not to the CREATE statement.
CREATE HANDLER h04303_fmt URL '/test_04303/fmt' AS SELECT 1 FORMAT JSONEachRow;
SELECT query, create_query FROM system.handlers WHERE name = 'h04303_fmt';
DROP HANDLER h04303_fmt;

-- PROTOCOL, URL PREFIX, METHODS and TYPE clauses.
CREATE HANDLER h04303_b PROTOCOL my_proto URL PREFIX '/test_04303/b/' METHODS (GET, POST) TYPE query AS SELECT 'b';
SELECT name, protocol, url_match_type, url, methods, type FROM system.handlers WHERE name = 'h04303_b';

-- All four HTTP methods are accepted.
CREATE HANDLER h04303_methods URL '/test_04303/m' METHODS (GET, POST, PUT, DELETE) AS SELECT 1;
SELECT methods FROM system.handlers WHERE name = 'h04303_methods';
DROP HANDLER h04303_methods;

-- URL REGEXP. The query is parsed but not analyzed: it may reference a non-existing table.
CREATE HANDLER h04303_c URL REGEXP '/test_04303/c/(?P<id>[0-9]+)' AS SELECT * FROM no_such_db.no_such_table;
SELECT name, url_match_type, url FROM system.handlers WHERE name = 'h04303_c';

-- Two overlapping REGEXP handlers are allowed: ambiguity cannot be checked for regexp.
CREATE HANDLER h04303_re1 URL REGEXP '/test_04303/re/.*' AS SELECT 1;
CREATE HANDLER h04303_re2 URL REGEXP '/test_04303/re/[0-9]+' AS SELECT 2;
SELECT count() FROM system.handlers WHERE name IN ('h04303_re1', 'h04303_re2');
DROP HANDLER h04303_re1;
DROP HANDLER h04303_re2;

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

-- ALTER into ambiguity with an existing handler must fail.
CREATE HANDLER h04303_amb1 URL '/test_04303/amb1' AS SELECT 1;
CREATE HANDLER h04303_amb2 URL '/test_04303/amb2' AS SELECT 1;
ALTER HANDLER h04303_amb2 URL '/test_04303/amb1'; -- { serverError AMBIGUOUS_HANDLER }
DROP HANDLER h04303_amb1;
DROP HANDLER h04303_amb2;

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
