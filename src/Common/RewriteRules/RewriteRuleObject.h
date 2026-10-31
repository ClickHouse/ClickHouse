#pragma once

#include <optional>

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

    /// Set when the rule was loaded from persisted storage but failed the template
    /// screening that `CREATE RULE` / `ALTER RULE` perform (a template written directly
    /// into the storage, or persisted before a screening rule was introduced). The rule
    /// stays visible and can be dropped, but the matcher must never apply it.
    void rejectOnLoad(String reason);
    const std::optional<String> & getLoadRejectionReason() const;

protected:
    ASTCreateRewriteRuleQuery create_query;
    std::optional<String> load_rejection_reason;
};

}
