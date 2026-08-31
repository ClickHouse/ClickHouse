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
extern const Event ASTFuzzerOracleGroupByKeyPermutationChecks;
extern const Event ASTFuzzerOracleDistinctViaGroupByChecks;
extern const Event ASTFuzzerOraclePrewhereEquivalenceChecks;
extern const Event ASTFuzzerOracleSkipIndexEquivalenceChecks;
extern const Event ASTFuzzerOracleSettingFlipSweepChecks;
extern const Event ASTFuzzerOracleCodecRoundtripChecks;
extern const Event ASTFuzzerOracleEngineEquivalenceChecks;
extern const Event ASTFuzzerOraclePartitionEquivalenceChecks;
extern const Event ASTFuzzerOracleLowCardinalityEquivalenceChecks;
extern const Event ASTFuzzerOracleSampleEquivalenceChecks;
extern const Event ASTFuzzerOracleProjectionEquivalenceChecks;
extern const Event ASTFuzzerOracleAggregateIfIdentityChecks;
extern const Event ASTFuzzerOracleNullIdentityChecks;
extern const Event ASTFuzzerOracleCastRoundtripChecks;
extern const Event ASTFuzzerOracleAggregateStateColumnChecks;
extern const Event ASTFuzzerOracleTupleSummingChecks;
extern const Event ASTFuzzerOracleSchemaRoundtripChecks;
extern const Event ASTFuzzerOracleDeleteMutationChecks;
extern const Event ASTFuzzerOracleUpdateMutationChecks;
extern const Event ASTFuzzerOracleMaterializeIndexChecks;
extern const Event ASTFuzzerOraclePredicateDeMorganChecks;
extern const Event ASTFuzzerOracleArrayJoinIdentityChecks;
extern const Event ASTFuzzerOracleGroupingSetsChecks;
extern const Event ASTFuzzerOracleRowPolicyChecks;
extern const Event ASTFuzzerOracleFinalMergeChecks;
extern const Event ASTFuzzerOracleWithFillChecks;
extern const Event ASTFuzzerOraclePipeEquivalenceChecks;
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
    add("GROUP BY key permutation", ProfileEvents::ASTFuzzerOracleGroupByKeyPermutationChecks, &QueryOracleChecker::checkGroupByKeyPermutation);
    add("DISTINCT via GROUP BY", ProfileEvents::ASTFuzzerOracleDistinctViaGroupByChecks, &QueryOracleChecker::checkDistinctViaGroupBy);
    add("PREWHERE equivalence", ProfileEvents::ASTFuzzerOraclePrewhereEquivalenceChecks, &QueryOracleChecker::checkPrewhereEquivalence);
    add("skip-index equivalence", ProfileEvents::ASTFuzzerOracleSkipIndexEquivalenceChecks, &QueryOracleChecker::checkSkipIndexEquivalence);
    add("setting-flip sweep", ProfileEvents::ASTFuzzerOracleSettingFlipSweepChecks, &QueryOracleChecker::checkSettingFlipSweep);
    /// Self-seeded (fixture-based) oracles run last.
    add("codec round-trip", ProfileEvents::ASTFuzzerOracleCodecRoundtripChecks, &QueryOracleChecker::checkCodecRoundtrip);
    add("engine equivalence", ProfileEvents::ASTFuzzerOracleEngineEquivalenceChecks, &QueryOracleChecker::checkEngineEquivalence);
    add("partition equivalence", ProfileEvents::ASTFuzzerOraclePartitionEquivalenceChecks, &QueryOracleChecker::checkPartitionEquivalence);
    add("LowCardinality equivalence", ProfileEvents::ASTFuzzerOracleLowCardinalityEquivalenceChecks, &QueryOracleChecker::checkLowCardinalityEquivalence);
    add("SAMPLE equivalence", ProfileEvents::ASTFuzzerOracleSampleEquivalenceChecks, &QueryOracleChecker::checkSampleEquivalence);
    add("projection equivalence", ProfileEvents::ASTFuzzerOracleProjectionEquivalenceChecks, &QueryOracleChecker::checkProjectionEquivalence);
    add("aggregate-If identity", ProfileEvents::ASTFuzzerOracleAggregateIfIdentityChecks, &QueryOracleChecker::checkAggregateIfIdentity);
    add("NULL identity", ProfileEvents::ASTFuzzerOracleNullIdentityChecks, &QueryOracleChecker::checkNullIdentity);
    add("CAST round-trip", ProfileEvents::ASTFuzzerOracleCastRoundtripChecks, &QueryOracleChecker::checkCastRoundtrip);
    add("aggregate-state column", ProfileEvents::ASTFuzzerOracleAggregateStateColumnChecks, &QueryOracleChecker::checkAggregateStateColumn);
    add("tuple summing", ProfileEvents::ASTFuzzerOracleTupleSummingChecks, &QueryOracleChecker::checkTupleSumming);
    add("schema round-trip", ProfileEvents::ASTFuzzerOracleSchemaRoundtripChecks, &QueryOracleChecker::checkSchemaRoundtrip);
    add("DELETE mutation", ProfileEvents::ASTFuzzerOracleDeleteMutationChecks, &QueryOracleChecker::checkDeleteMutation);
    add("UPDATE mutation", ProfileEvents::ASTFuzzerOracleUpdateMutationChecks, &QueryOracleChecker::checkUpdateMutation);
    add("MATERIALIZE INDEX invariance", ProfileEvents::ASTFuzzerOracleMaterializeIndexChecks, &QueryOracleChecker::checkMaterializeIndexInvariance);
    add("De-Morgan predicate", ProfileEvents::ASTFuzzerOraclePredicateDeMorganChecks, &QueryOracleChecker::checkPredicateDeMorgan);
    add("ARRAY JOIN identity", ProfileEvents::ASTFuzzerOracleArrayJoinIdentityChecks, &QueryOracleChecker::checkArrayJoinIdentity);
    add("grouping-set equivalence", ProfileEvents::ASTFuzzerOracleGroupingSetsChecks, &QueryOracleChecker::checkGroupingSetsEquivalence);
    add("row-policy equivalence", ProfileEvents::ASTFuzzerOracleRowPolicyChecks, &QueryOracleChecker::checkRowPolicyEquivalence);
    add("FINAL-merge dedup", ProfileEvents::ASTFuzzerOracleFinalMergeChecks, &QueryOracleChecker::checkFinalMergeReplacing);
    add("WITH FILL grid", ProfileEvents::ASTFuzzerOracleWithFillChecks, &QueryOracleChecker::checkWithFillGrid);
    add("pipe equivalence", ProfileEvents::ASTFuzzerOraclePipeEquivalenceChecks, &QueryOracleChecker::checkPipeEquivalence);
}

const OracleRegistry & OracleRegistry::instance()
{
    static const OracleRegistry registry;
    return registry;
}

}
