-- Tags: no-fasttest
-- no-fasttest: the URL table engine and url/urlCluster table functions are not in the fast test build.

-- Credentials in every URL carrier must be hidden when secrets are not displayed: the userinfo of the
-- url positional or a named `url = ...` override, and the `headers(...)` values, for the `url` and
-- `urlCluster` table functions (including their named-collection forms) and the `URL` table engine.
-- urlCluster puts the cluster name first, so the url is its second argument.

SET enable_analyzer = 1;
SET format_display_secrets_in_show_and_select = 0;

-- Function forms via the query-tree dump (run_passes = 0 keeps them unresolved, so no network access).
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url('https://user:SEKRIT_PW@localhost:11111/x', 'CSV', 'c UInt8', headers('Authorization' = 'SEKRIT_HDR'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM urlCluster('c', 'https://user:SEKRIT_PW@localhost:11111/x', 'CSV', 'c UInt8', headers('Authorization' = 'SEKRIT_HDR'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04648_missing, url = 'https://user:SEKRIT_PW@localhost:11111/x?token=SEKRIT_TOK', headers('Authorization' = 'SEKRIT_HDR'));

-- A url built from a constant expression is evaluated by the parser but not by the masker, so both a
-- positional and a named `url` override must fail closed (hidden whole) rather than leak the pieces.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(concat('https://user:', 'SEKRIT_EXPR@localhost:11111/x'), 'CSV', 'c UInt8');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04648_missing, url = concat('https://user:', 'SEKRIT_NAMEDEXPR@localhost:11111/x'));

-- A named-collection override key evaluated from a constant expression can name `url`, and a nested
-- map placed as the value of any visible non-url override (format, description, ...) carries a secret;
-- both are formatted before the collection is validated, so both must fail closed.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04648_missing, concat('u', 'rl') = concat('https://user:', 'SEKRIT_EXPRKEY@localhost/x'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04648_missing, format = headers('Authorization' = 'SEKRIT_URLFMTHDR'), structure = 'c UInt8');

-- The URL table engine: SHOW CREATE must hide the userinfo and the headers values.
DROP TABLE IF EXISTS t_04648_url;
CREATE TABLE t_04648_url (x UInt8)
ENGINE = URL('https://user:SEKRIT_PW@localhost:11111/x', 'CSV', headers('Authorization' = 'SEKRIT_HDR'));
SHOW CREATE TABLE t_04648_url SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04648_url;

-- A view over a URL source keeps its definition masked in SHOW CREATE, and querying the view logs
-- only the view name (the credential is never in the logged query text).
DROP TABLE IF EXISTS v_04648;
CREATE VIEW v_04648 AS SELECT * FROM url('https://user:SEKRIT_PW@localhost:11111/x', 'CSV', 'c UInt8');
SHOW CREATE TABLE v_04648 SETTINGS format_display_secrets_in_show_and_select = 0;
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM v_04648;
DROP TABLE v_04648;
