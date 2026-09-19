-- Tags: no-parallel
-- ^ Handlers are a global, server-wide namespace, and this test uses fixed handler names. The flaky
-- check runs several copies of the same test concurrently (`--test-runs N --jobs M`), so without this
-- tag a `DROP HANDLER IF EXISTS` in one copy removes a handler another copy is about to drop, leading
-- to `HANDLER_DOESNT_EXIST`. This mirrors how named collection tests (also a global namespace) are tagged.

-- Tests for CREATE/ALTER/DROP HANDLER: parsing, the registry, ambiguity checks,
-- type validation and the system.handlers introspection table.
-- Handler names/URLs are globally unique to this test to avoid cross-test interference.

DROP HANDLER IF EXISTS h04305_a;
DROP HANDLER IF EXISTS h04305_b;
DROP HANDLER IF EXISTS h04305_c;

-- Basic CREATE with an exact URL, default method (GET) and default type (query).
CREATE HANDLER h04305_a URL '/test_04305/a' AS SELECT 1 AS x;

SELECT name, protocol, url_match_type, url, methods, type, query
FROM system.handlers WHERE name = 'h04305_a';

-- CREATE IF NOT EXISTS must be a no-op when the handler already exists.
CREATE HANDLER IF NOT EXISTS h04305_a URL '/test_04305/other' AS SELECT 2;
SELECT url FROM system.handlers WHERE name = 'h04305_a';

-- Creating an existing handler without IF NOT EXISTS must fail.
CREATE HANDLER h04305_a URL '/test_04305/dup' AS SELECT 3; -- { serverError HANDLER_ALREADY_EXISTS }

-- URL is mandatory for CREATE.
CREATE HANDLER h04305_nourl AS SELECT 1; -- { clientError SYNTAX_ERROR }

-- The query is parsed for syntactic correctness at creation time.
CREATE HANDLER h04305_bad_query URL '/test_04305/badq' AS SELECT FROM WHERE; -- { clientError SYNTAX_ERROR }

-- The query can be put in parentheses for disambiguation; the parentheses are not stored.
CREATE HANDLER h04305_paren URL '/test_04305/paren' AS (SELECT 1);
SELECT query FROM system.handlers WHERE name = 'h04305_paren';
DROP HANDLER h04305_paren;

-- Arbitrary queries are supported, not only SELECT/INSERT.
CREATE HANDLER h04305_show URL '/test_04305/show' AS SHOW DATABASES;
SELECT query FROM system.handlers WHERE name = 'h04305_show';
DROP HANDLER h04305_show;

-- The FORMAT clause belongs to the query, not to the CREATE statement.
CREATE HANDLER h04305_fmt URL '/test_04305/fmt' AS SELECT 1 FORMAT JSONEachRow;
SELECT query, create_query FROM system.handlers WHERE name = 'h04305_fmt';
DROP HANDLER h04305_fmt;

-- UNION/INTERSECT/EXCEPT queries are formatted with parenthesized operands (e.g. `(SELECT 1) EXCEPT (SELECT 2)`).
-- The CREATE statement must format and parse back unchanged, both for the persisted statement (re-parsed on
-- reload) and for the AST fuzzer's format/parse consistency check.
CREATE HANDLER h04305_union URL '/test_04305/union' AS SELECT 1 UNION ALL SELECT 2;
SELECT query, create_query FROM system.handlers WHERE name = 'h04305_union';
DROP HANDLER h04305_union;

CREATE HANDLER h04305_except URL '/test_04305/except' AS SELECT 1 EXCEPT ALL SELECT 2;
SELECT query, create_query FROM system.handlers WHERE name = 'h04305_except';
DROP HANDLER h04305_except;

CREATE HANDLER h04305_intersect URL '/test_04305/intersect' AS SELECT 1 INTERSECT ALL SELECT 2;
SELECT query, create_query FROM system.handlers WHERE name = 'h04305_intersect';
DROP HANDLER h04305_intersect;

-- The operands may also be parenthesized in the source, which previously failed to parse because the leading
-- parenthesis was mistaken for the optional disambiguation wrapper around the whole query.
CREATE HANDLER h04305_union_paren URL '/test_04305/union_paren' AS (SELECT 1) UNION ALL (SELECT 2);
SELECT query FROM system.handlers WHERE name = 'h04305_union_paren';
DROP HANDLER h04305_union_paren;

-- PROTOCOL, URL PREFIX, METHODS and TYPE clauses.
CREATE HANDLER h04305_b PROTOCOL my_proto URL PREFIX '/test_04305/b/' METHODS (GET, POST) TYPE query AS SELECT 'b';
SELECT name, protocol, url_match_type, url, methods, type FROM system.handlers WHERE name = 'h04305_b';

-- All four HTTP methods are accepted.
CREATE HANDLER h04305_methods URL '/test_04305/m' METHODS (GET, POST, PUT, DELETE) AS SELECT 1;
SELECT methods FROM system.handlers WHERE name = 'h04305_methods';
DROP HANDLER h04305_methods;

-- A handler whose query modifies data must allow a mutating HTTP method (POST, PUT or DELETE).
-- With the default methods (GET only) such a handler is rejected at creation time.
CREATE HANDLER h04305_ins_get URL '/test_04305/ins_get' AS INSERT INTO no_such_db.no_such_table SELECT 1; -- { serverError BAD_ARGUMENTS }
-- The same, but with an explicit read-only method.
CREATE HANDLER h04305_ins_get URL '/test_04305/ins_get' METHODS (GET) AS INSERT INTO no_such_db.no_such_table SELECT 1; -- { serverError BAD_ARGUMENTS }
-- A mutating query is accepted when a mutating method (here PUT) is allowed.
CREATE HANDLER h04305_ins_put URL '/test_04305/ins_put' METHODS (PUT) AS INSERT INTO no_such_db.no_such_table SELECT 1;
SELECT name, methods FROM system.handlers WHERE name = 'h04305_ins_put';
DROP HANDLER h04305_ins_put;
-- A handler reading the request body (an INSERT taking its data from the body, or a query using
-- `_request_body`) must not allow any read-only method: a safe method can never carry a body, and a declared
-- GET is also served for HEAD. An `INSERT ... SELECT ... FROM input(...)` reads the body as well.
CREATE HANDLER h04305_ins_mix URL '/test_04305/ins_mix' METHODS (GET, DELETE) AS INSERT INTO no_such_db.no_such_table SELECT * FROM input('x String'); -- { serverError BAD_ARGUMENTS }
CREATE HANDLER h04305_body_mix URL '/test_04305/body_mix' METHODS (GET, POST) AS SELECT {_request_body:String}; -- { serverError BAD_ARGUMENTS }
-- Mixing several body-carrying methods is fine.
CREATE HANDLER h04305_ins_mix URL '/test_04305/ins_mix' METHODS (POST, DELETE) AS INSERT INTO no_such_db.no_such_table SELECT * FROM input('x String');
SELECT name, methods FROM system.handlers WHERE name = 'h04305_ins_mix';
-- ALTER that would leave a mutating handler with only read-only methods is rejected.
ALTER HANDLER h04305_ins_mix METHODS (GET); -- { serverError BAD_ARGUMENTS }
-- The same for an ALTER that adds a read-only method to a body-reading handler.
ALTER HANDLER h04305_ins_mix METHODS (GET, POST); -- { serverError BAD_ARGUMENTS }
DROP HANDLER h04305_ins_mix;
-- Mixing read-only and mutating methods is fine for a mutating query that does not read the request body.
CREATE HANDLER h04305_ddl_mix URL '/test_04305/ddl_mix' METHODS (GET, DELETE) AS TRUNCATE TABLE no_such_db.no_such_table;
SELECT name, methods FROM system.handlers WHERE name = 'h04305_ddl_mix';
DROP HANDLER h04305_ddl_mix;
-- `INSERT ... SELECT` takes its data from the SELECT, not from the HTTP body, so it is not body-reading:
-- it needs a mutating method, but it may also allow read-only ones.
CREATE HANDLER h04305_ins_sel_mix URL '/test_04305/ins_sel_mix' METHODS (GET, DELETE) AS INSERT INTO no_such_db.no_such_table SELECT 1;
SELECT name, methods FROM system.handlers WHERE name = 'h04305_ins_sel_mix';
ALTER HANDLER h04305_ins_sel_mix METHODS (GET, POST);
SELECT name, methods FROM system.handlers WHERE name = 'h04305_ins_sel_mix';
DROP HANDLER h04305_ins_sel_mix;
-- ALTER that turns a read-only handler's query into a mutating one, without a mutating method, is rejected.
CREATE HANDLER h04305_ro URL '/test_04305/ro' AS SELECT 1;
ALTER HANDLER h04305_ro AS INSERT INTO no_such_db.no_such_table SELECT 1; -- { serverError BAD_ARGUMENTS }
DROP HANDLER h04305_ro;

-- URL REGEXP. The query is parsed but not analyzed: it may reference a non-existing table.
CREATE HANDLER h04305_c URL REGEXP '/test_04305/c/(?P<id>[0-9]+)' AS SELECT * FROM no_such_db.no_such_table;
SELECT name, url_match_type, url FROM system.handlers WHERE name = 'h04305_c';

-- Two overlapping REGEXP handlers are allowed: ambiguity cannot be checked for regexp.
CREATE HANDLER h04305_re1 URL REGEXP '/test_04305/re/.*' AS SELECT 1;
CREATE HANDLER h04305_re2 URL REGEXP '/test_04305/re/[0-9]+' AS SELECT 2;
SELECT count() FROM system.handlers WHERE name IN ('h04305_re1', 'h04305_re2');
DROP HANDLER h04305_re1;
DROP HANDLER h04305_re2;

-- Unsupported handler type must fail.
CREATE HANDLER h04305_bad URL '/test_04305/bad' TYPE static AS SELECT 1; -- { serverError BAD_ARGUMENTS }

-- Unknown HTTP method must fail to parse.
CREATE HANDLER h04305_bad URL '/test_04305/bad' METHODS (TRACE) AS SELECT 1; -- { clientError SYNTAX_ERROR }

-- Ambiguity: exact URL equal to an existing exact URL with overlapping method and protocol.
CREATE HANDLER h04305_dup_exact URL '/test_04305/a' AS SELECT 1; -- { serverError AMBIGUOUS_HANDLER }

-- Ambiguity: a prefix that covers an existing exact URL (same default GET method).
CREATE HANDLER h04305_dup_prefix URL PREFIX '/test_04305/' AS SELECT 1; -- { serverError AMBIGUOUS_HANDLER }

-- Not ambiguous: same URL but disjoint methods.
CREATE HANDLER h04305_post_a URL '/test_04305/a' METHODS (POST) AS SELECT 1;
SELECT name, methods FROM system.handlers WHERE name = 'h04305_post_a';
DROP HANDLER h04305_post_a;

-- ALTER: change only the query (other clauses are preserved).
ALTER HANDLER h04305_a AS SELECT 42 AS y;
SELECT url, methods, query FROM system.handlers WHERE name = 'h04305_a';

-- ALTER: change only the URL.
ALTER HANDLER h04305_a URL PREFIX '/test_04305/a_new';
SELECT url_match_type, url, query FROM system.handlers WHERE name = 'h04305_a';

-- ALTER: change methods.
ALTER HANDLER h04305_a METHODS (GET, POST, PUT, DELETE);
SELECT methods FROM system.handlers WHERE name = 'h04305_a';

-- ALTER: restrict the handler to a protocol, then remove the restriction with PROTOCOL ANY.
ALTER HANDLER h04305_a PROTOCOL some_proto_04305;
SELECT protocol FROM system.handlers WHERE name = 'h04305_a';
ALTER HANDLER h04305_a PROTOCOL ANY;
SELECT protocol, create_query FROM system.handlers WHERE name = 'h04305_a';

-- PROTOCOL ANY on CREATE is the same as omitting the clause and is normalized away.
CREATE HANDLER h04305_pany PROTOCOL ANY URL '/test_04305/pany' AS SELECT 1;
SELECT protocol, create_query FROM system.handlers WHERE name = 'h04305_pany';
DROP HANDLER h04305_pany;

-- A protocol literally named "any" is referenced with back quotes and survives the
-- format/parse round-trip of the persisted statement (it is not confused with PROTOCOL ANY).
CREATE HANDLER h04305_pliteral PROTOCOL `any` URL '/test_04305/pliteral' AS SELECT 1;
SELECT protocol, create_query FROM system.handlers WHERE name = 'h04305_pliteral';
DROP HANDLER h04305_pliteral;

-- ALTER into ambiguity with an existing handler must fail.
CREATE HANDLER h04305_amb1 URL '/test_04305/amb1' AS SELECT 1;
CREATE HANDLER h04305_amb2 URL '/test_04305/amb2' AS SELECT 1;
ALTER HANDLER h04305_amb2 URL '/test_04305/amb1'; -- { serverError AMBIGUOUS_HANDLER }
DROP HANDLER h04305_amb1;
DROP HANDLER h04305_amb2;

-- ALTER a non-existing handler must fail.
ALTER HANDLER h04305_absent AS SELECT 1; -- { serverError HANDLER_DOESNT_EXIST }

-- DROP a non-existing handler without IF EXISTS must fail.
DROP HANDLER h04305_absent; -- { serverError HANDLER_DOESNT_EXIST }

-- DROP IF EXISTS on a non-existing handler is a no-op.
DROP HANDLER IF EXISTS h04305_absent;

-- Count of this test's handlers, then clean up.
SELECT count() FROM system.handlers WHERE name LIKE 'h04305\_%';

DROP HANDLER h04305_a;
DROP HANDLER h04305_b;
DROP HANDLER h04305_c;

SELECT count() FROM system.handlers WHERE name LIKE 'h04305\_%';
