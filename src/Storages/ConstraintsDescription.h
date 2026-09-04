#pragma once

#include <Analyzer/Passes/CNFAtomicFormula.h>
#include <Interpreters/CNFQueryAtomicFormula.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IASTHash.h>

#include <map>
#include <memory>
#include <optional>
#include <vector>


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

class NamesAndTypesList;

struct ConstraintsDescription
{
public:
    ConstraintsDescription() { update(); }
    explicit ConstraintsDescription(const ASTs & constraints_);
    ConstraintsDescription(const ConstraintsDescription & other);
    ConstraintsDescription & operator=(const ConstraintsDescription & other);

    /// Not noexcept: the move operations rebuild the derived data via update(), which allocates
    /// (make_unique, CNF construction) and can throw, e.g. MEMORY_LIMIT_EXCEEDED.
    ConstraintsDescription(ConstraintsDescription && other); /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)
    ConstraintsDescription & operator=(ConstraintsDescription && other); /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)

    bool empty() const { return constraints.empty(); }
    String toString() const;

    static ConstraintsDescription parse(const String & str);

    enum class ConstraintType : UInt8
    {
        CHECK = 1,
        ASSUME = 2,
        ALWAYS_TRUE = CHECK | ASSUME,
        ALL = CHECK | ASSUME,
    };

    ASTs filterConstraints(ConstraintType selection) const;

    const ASTs & getConstraints() const;

    const std::vector<std::vector<CNFQueryAtomicFormula>> & getConstraintData() const;
    std::vector<CNFQueryAtomicFormula> getAtomicConstraintData() const;

    const ComparisonGraph<ASTPtr> & getGraph() const;

    ConstraintsExpressions getExpressions(ContextPtr context, const NamesAndTypesList & source_columns_) const;

    /// Rejects a `CHECK` expression that changes the number of rows. `CheckConstraintsTransform` reads
    /// the constraint's result column by block row, so an `arrayJoin` inside it makes a row be checked
    /// against another row's value, or - when the column ends up shorter than the block - past the end of
    /// it. The declaration's AST is read rather than the built expression, because a constraint that
    /// cannot be built at all (a bare subquery as in `03594_constraint_subqery_logical_error`, a wrong
    /// arity as in `04489_constraint_comparison_wrong_arity`) is only reported when a row is inserted, and
    /// building it here would move that report to the DDL.
    /// Called from DDL only, so metadata stored before this check still loads.
    void checkExpressionsPreserveRowCount() const;

    struct AtomId
    {
        size_t group_id;
        size_t atom_id;
    };

    using AtomIds = std::vector<AtomId>;

    std::optional<AtomIds> getAtomIds(const ASTPtr & ast) const;
    std::vector<CNFQueryAtomicFormula> getAtomsById(const AtomIds & ids) const;

    class QueryTreeData
    {
    public:
        const QueryTreeNodes & getConstraints() const;
        const std::vector<std::vector<Analyzer::CNFAtomicFormula>> & getConstraintData() const;
        std::optional<AtomIds> getAtomIds(const QueryTreeNodePtrWithHash & node_with_hash) const;
        std::vector<Analyzer::CNFAtomicFormula> getAtomsById(const AtomIds & ids) const;
        const ComparisonGraph<QueryTreeNodePtr> & getGraph() const;
    private:
        QueryTreeNodes constraints;
        std::vector<std::vector<Analyzer::CNFAtomicFormula>> cnf_constraints;
        QueryTreeNodePtrWithHashMap<AtomIds> query_node_to_atom_ids;
        std::unique_ptr<ComparisonGraph<QueryTreeNodePtr>> graph;

        friend ConstraintsDescription;
    };

    QueryTreeData getQueryTreeData(const ContextPtr & context, const TableExpressionNodePtr & table_node) const;

private:
    std::vector<std::vector<CNFQueryAtomicFormula>> buildConstraintData() const;
    std::unique_ptr<ComparisonGraph<ASTPtr>> buildGraph() const;
    void update();

    ASTs constraints;

    std::vector<std::vector<CNFQueryAtomicFormula>> cnf_constraints;
    std::map<IASTHash, AtomIds> ast_to_atom_ids;

    std::unique_ptr<ComparisonGraph<ASTPtr>> graph;
};

}
