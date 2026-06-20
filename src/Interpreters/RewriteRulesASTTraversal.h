#pragma once
#include <Common/RewriteRules/RewriteRules.h>


namespace DB
{

/// Applies the rewrite rules listed in the `query_rules` setting to `ast` (matching queries
/// are rewritten in place or rejected via an exception). The names of the rules that were
/// applied are appended to `applied_rules`, in application order, so the caller can record
/// them in `system.query_log` (this also happens before a `REJECT` rule throws). Returns
/// whether any rules were active.
bool astTraversal(ASTPtr& ast, ContextPtr context, std::vector<String> & applied_rules);
void applyRule(ASTPtr& ast, RewriteRuleObjectPtr rule, std::unordered_map<String, ASTPtr>& matching_map);

/// Enforces `max_ast_depth` / `max_ast_elements` on a rewrite rule's source and
/// result templates. They are stored outside `IAST::children`, so the generic
/// `checkASTSizeLimits` in `executeQuery` does not traverse them and a `CREATE RULE`
/// / `ALTER RULE` with a huge or very deep template would otherwise be accepted.
void checkRewriteRuleTemplateLimits(const ASTPtr & source_query, const ASTPtr & resulting_query, ContextPtr context);

};
