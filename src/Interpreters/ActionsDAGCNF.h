#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsDAGHashUtils.h>
#include <Common/CNFAtomicFormula.h>
#include <Common/CNFHelpers.h>
#include <Interpreters/Context_fwd.h>

#include <set>
#include <functional>
#include <optional>

namespace DB
{

/** Conjunctive Normal Form (CNF) representation for ActionsDAG filters.
  *
  * CNF is a conjunction (AND) of disjunctions (OR) of atomic formulas.
  * For example: (a OR b) AND (c OR NOT d) AND e
  *
  * This is useful for:
  * - Index analysis and optimization
  * - Filter pushdown and splitting
  * - Logical optimization of complex conditions
  *
  * Based on the QueryTree CNF implementation but adapted for ActionsDAG.
  */
class ActionsDAGCNF
{
public:
    using AtomicFormula = CNFAtomicFormula<ActionsDAGNodeWithHash>;
    using OrGroup = std::set<AtomicFormula>;
    using AndGroup = std::set<OrGroup>;

    static constexpr size_t DEFAULT_MAX_GROWTH_MULTIPLIER = 20;
    static constexpr size_t MAX_ATOMS_WITHOUT_CHECK = 200;

    explicit ActionsDAGCNF(AndGroup statements_);
    ActionsDAGCNF(AndGroup statements_, std::shared_ptr<ActionsDAG> dag_);

    /// Build CNF from a single ActionsDAG output node (typically a filter condition)
    /// Returns nullopt if the CNF would be too large (exceeds max_growth_multiplier)
    static std::optional<ActionsDAGCNF> tryBuildCNF(
        const ActionsDAG::Node * node,
        ContextPtr context,
        size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);

    /// Build CNF from a filter node, throws if CNF is too large
    static ActionsDAGCNF toCNF(
        const ActionsDAG::Node * node,
        ContextPtr context,
        size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);

    /// Convert CNF back to ActionsDAG
    /// Returns a new DAG with the CNF formula as a single output node
    ActionsDAG toActionsDAG(ContextPtr context) const;

    /// Get the CNF root node that can be added to an existing DAG
    const ActionsDAG::Node * toCNFNode(ActionsDAG & dag, ContextPtr context) const;

    /// Transform each atomic formula using the provided function
    ActionsDAGCNF & transformAtoms(std::function<AtomicFormula(const AtomicFormula &)> fn);

    /// Transform each OR group using the provided function
    ActionsDAGCNF & transformGroups(std::function<OrGroup(const OrGroup &)> fn);

    /// Filter out OR groups that are always true
    ActionsDAGCNF & filterAlwaysTrueGroups(std::function<bool(const OrGroup &)> predicate);

    /// Filter out atomic formulas that are always false
    ActionsDAGCNF & filterAlwaysFalseAtoms(std::function<bool(const AtomicFormula &)> predicate);

    /// Reduce CNF by applying logical simplification rules
    ActionsDAGCNF & reduce();

    /// Append another CNF (conjunction)
    void appendGroup(const AndGroup & and_group);

    /// Convert "NOT fn" to a single node representing inverse of "fn"
    /// For example: NOT(a = b) -> a != b
    ActionsDAGCNF & pushNotIntoFunctions(ContextPtr context);

    /// Pull NOT out of functions (reverse of pushNotIntoFunctions)
    /// For example: a != b -> NOT(a = b)
    ActionsDAGCNF & pullNotOutFunctions(ContextPtr context);

    /// Push NOT into a single atomic formula
    static AtomicFormula pushNotIntoFunction(const AtomicFormula & atom, ActionsDAG & dag, ContextPtr context);

    /// Iterate over all OR groups
    template <typename F>
    void iterateGroups(F func) const
    {
        for (const auto & group : statements)
            func(group);
    }

    /// Get the statements
    const AndGroup & getStatements() const { return statements; }

    /// Get the owned DAG (may be null if CNF was constructed without a DAG)
    std::shared_ptr<ActionsDAG> getOwnedDag() const { return owned_dag; }

    /// Dump CNF as a human-readable string
    std::string dump() const;

private:
    AndGroup statements;
    /// Internal DAG that owns the nodes referenced by statements
    /// This DAG must outlive the statements to keep node pointers valid
    std::shared_ptr<ActionsDAG> owned_dag;

    friend class std::optional<ActionsDAGCNF>;
};

}
