#include <Common/RewriteRules/RewriteRuleObject.h>


namespace DB
{


RewriteRuleObject::RewriteRuleObject(const ASTCreateRewriteRuleQuery& query_)
: create_query(query_.clone()->as<ASTCreateRewriteRuleQuery &>())
{
}

RewriteRuleObject::RewriteRuleObject(const ASTAlterRewriteRuleQuery& query_)
: create_query()
{
    auto copy = query_.clone()->as<ASTAlterRewriteRuleQuery &>();
    create_query.rule_name = std::move(copy.rule_name);
    create_query.source_query = std::move(copy.source_query);
    create_query.resulting_query = std::move(copy.resulting_query);
    create_query.reject_message = std::move(copy.reject_message);
    create_query.whole_query = "CREATE " + copy.whole_query.substr(sizeof("ALTER ") - 1);
}

MutableRewriteRuleObjectPtr RewriteRuleObject::create(const ASTCreateRewriteRuleQuery& query_)
{
    return std::make_shared<RewriteRuleObject>(query_);
}

MutableRewriteRuleObjectPtr RewriteRuleObject::create(const ASTAlterRewriteRuleQuery& query_)
{
    return std::make_shared<RewriteRuleObject>(query_);
}

const ASTCreateRewriteRuleQuery& RewriteRuleObject::getCreateQuery() const
{
    return create_query;
}

RewriteRuleLog::RewriteRuleLog(
    const std::string& original_query_,
    const Array& applied_rules_,
    const std::string& resulting_query_
) : original_query(original_query_), applied_rules(applied_rules_), resulting_query(resulting_query_)
{
}

MutableRewriteRuleLogPtr RewriteRuleLog::create(
    const std::string& original_query,
    const Array& applied_rules,
    const std::string& resulting_query
)
{
    return std::make_shared<RewriteRuleLog>(original_query, applied_rules, resulting_query);
}

}
