#include <Interpreters/QueryOracles/OracleRegistry.h>

#include <Interpreters/QueryOracleChecker.h>

namespace ProfileEvents
{
extern const Event ASTFuzzerOracleTLPWhereChecks;
extern const Event ASTFuzzerOracleNoRECChecks;
extern const Event ASTFuzzerOracleTLPAggregateChecks;
extern const Event ASTFuzzerOracleTLPDistinctChecks;
extern const Event ASTFuzzerOracleTLPGroupByChecks;
extern const Event ASTFuzzerOracleTLPHavingChecks;
extern const Event ASTFuzzerOracleDQPChecks;
extern const Event ASTFuzzerOracleIdentityWhereChecks;
extern const Event ASTFuzzerOracleSubqueryWrapChecks;
}

namespace DB
{

namespace
{

using CheckMethod = bool (QueryOracleChecker::*)(const ASTSelectQuery &, const ContextMutablePtr &);

/// Phase-0 adapter: delegates to an existing `QueryOracleChecker::check*` method and,
/// when it reports a comparison was performed, increments the oracle's own ProfileEvent.
/// The umbrella `ASTFuzzerOracleChecks` counter is still incremented inside each `check*`.
class CheckMethodOracle : public IOracle
{
public:
    CheckMethodOracle(OracleTraits traits_, CheckMethod method_)
        : oracle_traits(traits_), method(method_)
    {
    }

    const OracleTraits & traits() const override { return oracle_traits; }

    bool run(QueryOracleChecker & checker, const ASTSelectQuery & select, const ContextMutablePtr & context) const override
    {
        const bool performed = (checker.*method)(select, context);
        if (performed)
            ProfileEvents::increment(oracle_traits.event);
        return performed;
    }

private:
    OracleTraits oracle_traits;
    CheckMethod method;
};

}

OracleRegistry::OracleRegistry()
{
    auto add = [&](std::string_view name, ProfileEvents::Event event, CheckMethod method)
    {
        oracle_list.push_back(std::make_unique<CheckMethodOracle>(OracleTraits{name, event}, method));
    };

    /// Order matches the historical dispatch order in `QueryOracleChecker::check`.
    add("TLP WHERE", ProfileEvents::ASTFuzzerOracleTLPWhereChecks, &QueryOracleChecker::checkTLPWhere);
    add("NoREC", ProfileEvents::ASTFuzzerOracleNoRECChecks, &QueryOracleChecker::checkNoREC);
    add("TLP Aggregate", ProfileEvents::ASTFuzzerOracleTLPAggregateChecks, &QueryOracleChecker::checkTLPAggregate);
    add("TLP DISTINCT", ProfileEvents::ASTFuzzerOracleTLPDistinctChecks, &QueryOracleChecker::checkTLPDistinct);
    add("TLP GROUP BY", ProfileEvents::ASTFuzzerOracleTLPGroupByChecks, &QueryOracleChecker::checkTLPGroupBy);
    add("TLP HAVING", ProfileEvents::ASTFuzzerOracleTLPHavingChecks, &QueryOracleChecker::checkTLPHaving);
    add("DQP", ProfileEvents::ASTFuzzerOracleDQPChecks, &QueryOracleChecker::checkDQP);
    add("Identity WHERE", ProfileEvents::ASTFuzzerOracleIdentityWhereChecks, &QueryOracleChecker::checkIdentityWhere);
    add("Subquery wrap", ProfileEvents::ASTFuzzerOracleSubqueryWrapChecks, &QueryOracleChecker::checkSubqueryWrap);
}

const OracleRegistry & OracleRegistry::instance()
{
    static const OracleRegistry registry;
    return registry;
}

}
