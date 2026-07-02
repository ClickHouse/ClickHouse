#pragma once

#include <Common/RewriteRules/RewriteRules_fwd.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/ASTDropRewriteRuleQuery.h>

namespace DB
{

class RewriteRuleObject
{
public:
    explicit RewriteRuleObject(const ASTCreateRewriteRuleQuery& query_);
    explicit RewriteRuleObject(const ASTAlterRewriteRuleQuery& query_);

    static MutableRewriteRuleObjectPtr create(const ASTCreateRewriteRuleQuery& query_);
    static MutableRewriteRuleObjectPtr create(const ASTAlterRewriteRuleQuery& query_);

    const ASTCreateRewriteRuleQuery& getCreateQuery() const;

protected:
    ASTCreateRewriteRuleQuery create_query;
};

}
