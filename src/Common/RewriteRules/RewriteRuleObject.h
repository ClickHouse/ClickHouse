#pragma once

#include <Common/RewriteRules/RewriteRules_fwd.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/ASTDropRewriteRuleQuery.h>
#include <Core/Field.h>

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

class RewriteRuleLog
{
public:
    explicit RewriteRuleLog(
        const std::string& original_query_,
        const Array& applied_rules_,
        const std::string& resulting_query_
    );

    static MutableRewriteRuleLogPtr create(
        const std::string& original_query_,
        const Array& applied_rules_,
        const std::string& resulting_query_
    );

    std::string original_query;
    Array applied_rules;
    std::string resulting_query;
};

}
