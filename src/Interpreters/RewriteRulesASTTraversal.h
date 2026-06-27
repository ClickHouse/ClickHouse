#pragma once
#include <Common/RewriteRules/RewriteRules.h>


namespace DB
{

struct Settings;

/// Applies the rewrite rules listed in the `query_rules` setting to `ast` (matching queries
/// are rewritten in place or rejected via an exception). The names of the rules that were
/// applied are appended to `applied_rules`, in application order, so the caller can record
/// them in `system.query_log` (this also happens before a `REJECT` rule throws). Returns
/// whether any rules were active.
bool astTraversal(ASTPtr& ast, ContextPtr context, std::vector<String> & applied_rules);
void applyRule(ASTPtr& ast, RewriteRuleObjectPtr rule, std::unordered_map<String, ASTPtr>& matching_map);

/// Enforces `max_ast_depth` / `max_ast_elements` on every rewrite-rule source/result
/// template reachable from `ast` (including rule DDL nested below a wrapper such as
/// `EXPLAIN AST CREATE RULE ...`). Those templates are stored outside `IAST::children`,
/// so the generic `checkASTSizeLimits` walk in `executeQuery` does not reach them and a
/// `CREATE RULE` / `ALTER RULE` with a huge or very deep template would otherwise be
/// accepted. Call this from the generic pre-execution AST limit gate so the check runs
/// before access/interpreter dispatch and cannot be bypassed via wrappers.
void checkRewriteRuleTemplateLimits(const IAST & ast, const Settings & settings);

};
