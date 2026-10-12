-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A query parameter in an AST member kept outside `children` — the `LIMIT` / `WHERE` of a `SHOW`
-- statement — must be substituted before rule matching, so a rule written against the bare
-- literal matches the parameterized spelling too: a `REJECT` rule must not be bypassable by
-- passing the matched value through a query parameter (`forEachNonChildSemanticAST` keeps the
-- substitution walk and the matcher on the same carrier list, and the matcher sees through the
-- typed-parameter `_CAST` wrapper inside such members).

CREATE RULE rule_04848_limit AS (SHOW TABLES LIMIT 42) REJECT WITH 'blocked';
SET query_rules = 'rule_04848_limit';

-- A typed parameter substitutes into `ASTShowTablesQuery::limit_length` as a `_CAST` wrapper.
SET param_n_04848 = 42;
SHOW TABLES LIMIT {n_04848:UInt64}; -- { serverError REWRITE_RULE_REJECTION }
SHOW TABLES LIMIT 42; -- { serverError REWRITE_RULE_REJECTION }

-- A different value must not match: the parameter is matched as the literal it became.
SET param_m_04848 = 43;
SHOW TABLES LIMIT {m_04848:UInt64};

SET query_rules = '';
DROP RULE rule_04848_limit;

-- The `WHERE` carrier: a `String` parameter substitutes as a bare literal.
CREATE RULE rule_04848_where AS (SHOW TABLES WHERE name = 'blocked_04848') REJECT WITH 'blocked';
SET query_rules = 'rule_04848_where';

SET param_s_04848 = 'blocked_04848';
SHOW TABLES WHERE name = {s_04848:String}; -- { serverError REWRITE_RULE_REJECTION }
SHOW TABLES WHERE name = 'blocked_04848'; -- { serverError REWRITE_RULE_REJECTION }
SHOW TABLES WHERE name = 'other_04848';

SET query_rules = '';
DROP RULE rule_04848_where;
