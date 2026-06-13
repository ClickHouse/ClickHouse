#pragma once
#include <Common/RewriteRules/RewriteRules.h>


namespace DB
{

bool astTraversal(ASTPtr& ast, ContextPtr context);
void applyRule(ASTPtr& ast, RewriteRuleObjectPtr rule, std::unordered_map<String, ASTPtr>& matching_map);

/// Enforces `max_ast_depth` / `max_ast_elements` on a rewrite rule's source and
/// result templates. They are stored outside `IAST::children`, so the generic
/// `checkASTSizeLimits` in `executeQuery` does not traverse them and a `CREATE RULE`
/// / `ALTER RULE` with a huge or very deep template would otherwise be accepted.
void checkRewriteRuleTemplateLimits(const ASTPtr & source_query, const ASTPtr & resulting_query, ContextPtr context);

};
