#pragma once

#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <Common/SipHash.h>

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
        bool operator<(const AtomicFormula & rhs) const;
    };

    struct SetAtomicFormulaHash
    {
        size_t operator()(const std::set<AtomicFormula> & or_group) const
        {
            SipHash hash;
            for (const auto & atomic_formula : or_group)
            {
                SipHash atomic_formula_hash;
                atomic_formula_hash.update(atomic_formula.negative);
                atomic_formula_hash.update(atomic_formula.node_with_hash.hash);

                hash.update(atomic_formula_hash.get64());
            }
            
            return hash.get64();
        }
    };

    // Different hash is generated for different order, so we use std::set
    using OrGroup = std::set<AtomicFormula>;
    using AndGroup = std::unordered_set<OrGroup, SetAtomicFormulaHash>;

    std::string dump() const;

    static constexpr size_t DEFAULT_MAX_GROWTH_MULTIPLIER = 20;
    static constexpr size_t MAX_ATOMS_WITHOUT_CHECK = 200;

    CNF & transformAtoms(std::function<AtomicFormula(const AtomicFormula &)> fn);

    /// Convert "NOT fn" to a single node representing inverse of "fn"
    CNF & pushNotIntoFunctions(const ContextPtr & context);

    static std::optional<CNF> tryBuildCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);

	QueryTreeNodePtr toQueryTree(ContextPtr context) const;
private:
    explicit CNF(AndGroup statements_);

    CNF & transformGroups(std::function<OrGroup(const OrGroup &)> fn);
    AndGroup statements;
};

}
