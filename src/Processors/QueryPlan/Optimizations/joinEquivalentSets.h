#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <unordered_map>
#include <vector>

namespace DB
{

class JoinStepLogical;
struct JoinOperator;

namespace QueryPlanOptimizations
{

/// Union-find over JoinActionRef. Used to discover columns and expressions
/// that must hold equal values across all rows in a JOIN
class EquivalentJoinKeySet
{
public:
    JoinActionRef findOrAdd(JoinActionRef ref);
    JoinActionRef unite(JoinActionRef a, JoinActionRef b);
    bool connected(JoinActionRef a, JoinActionRef b);
    std::unordered_map<JoinActionRef, std::vector<JoinActionRef>> getClasses();
    std::vector<JoinActionRef> getClass(JoinActionRef ref);

private:
    std::unordered_map<JoinActionRef, JoinActionRef> parent;
    std::unordered_map<JoinActionRef, size_t> rank;
};

using JoinActionRefPair = std::pair<JoinActionRef, JoinActionRef>;

struct JoinActionRefPairHash
{
    size_t operator()(const JoinActionRefPair & p) const noexcept
    {
        return std::hash<JoinActionRef>{}(p.first) ^ std::hash<JoinActionRef>{}(p.second);
    }
};

std::vector<JoinActionRefPair> getJoiningKeysForJoinStep(const JoinOperator & join_operator);

std::vector<JoinActionRefPair> buildEquialentSetsForJoinStepLogical(
    EquivalentJoinKeySet & equivalent_sets,
    const JoinStepLogical * join_step,
    const std::vector<QueryPlan::Node *> & child_nodes,
    int lookup_depth = 0);

}

}
