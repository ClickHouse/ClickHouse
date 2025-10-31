#pragma once
#include <Interpreters/Context_fwd.h>

#include <Common/RewriteRules/RewriteRuleObject.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/ASTDropRewriteRuleQuery.h>

namespace DB
{

class RewriteRulesStorage : private WithContext
{
public:
    static std::unique_ptr<RewriteRulesStorage> create(const ContextPtr & context);

    RewriteRuleObjectsMap getAll() const;

    MutableRewriteRuleObjectPtr get(const std::string & rule_name) const;

    void create(const RewriteRuleObjectPtr & query);

    void remove(const std::string & rule_name);

    bool removeIfExists(const std::string & rule_name);

    void update(const RewriteRuleObjectPtr & query);

    void shutdown();

    /// Return true if update was made
    bool waitUpdate();

    bool isReplicated() const;

private:
    class IRewriteRulesStorage;
    class LocalStorage;
    class ZooKeeperStorage;

    std::shared_ptr<IRewriteRulesStorage> impl_storage;

    RewriteRulesStorage(std::shared_ptr<IRewriteRulesStorage> storage_, ContextPtr context_);

    std::vector<std::string> listRules() const;

    ASTCreateRewriteRuleQuery readCreateQuery(const std::string & rule_name) const;

    void writeCreateQuery(const String & rule_name, const String & create_query, bool replace = false);
};


}
