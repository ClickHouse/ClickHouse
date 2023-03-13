#pragma once

#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <Interpreters/Context_fwd.h>

#include <unordered_set>


namespace DB::Analyzer
{

class CNF
{
public:
    struct AtomicFormula
    {
        bool negative = false;
        QueryTreeNodePtrWithHash node_with_hash;

        bool operator==(const AtomicFormula & rhs) const;
    };

    using OrGroup = std::unordered_set<AtomicFormula>;
    using AndGroup = std::unordered_set<OrGroup>;

    std::string dump() const;

    static constexpr size_t DEFAULT_MAX_GROWTH_MULTIPLIER = 20;

    static std::optional<CNF> tryBuildCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);
private:
    AndGroup statements;
};
 
}

template <>
struct std::hash<DB::Analyzer::CNF::AtomicFormula>
{
    size_t operator()(const DB::Analyzer::CNF::AtomicFormula & atomic_formula) const
    {
        return std::hash<DB::QueryTreeNodePtrWithHash>()(atomic_formula.node_with_hash);
    }
};
