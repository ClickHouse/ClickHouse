#pragma once
#include <Common/RewriteRules/RewriteRules.h>


namespace DB
{

bool astTraversal(ASTPtr& ast, ContextPtr context);
void applyRule(ASTPtr& ast, RewriteRuleObjectPtr rule, std::unordered_map<String, ASTPtr>& matching_map);

};
