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

    ConstraintsDescription(ConstraintsDescription && other) noexcept;
    ConstraintsDescription & operator=(ConstraintsDescription && other) noexcept;

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

    QueryTreeData getQueryTreeData(const ContextPtr & context, const QueryTreeNodePtr & table_node) const;

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
