#pragma once

#include <Parsers/ASTConstraintDeclaration.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>

#include <Analyzer/Passes/CNF.h>

namespace DB
{

using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

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

    const std::vector<std::vector<CNFQuery::AtomicFormula>> & getConstraintData() const;
    std::vector<CNFQuery::AtomicFormula> getAtomicConstraintData() const;

    const ComparisonGraph<ASTPtr> & getGraph() const;

    ConstraintsExpressions getExpressions(ContextPtr context, const NamesAndTypesList & source_columns_) const;

    struct AtomId
    {
        size_t group_id;
        size_t atom_id;
    };

    using AtomIds = std::vector<AtomId>;

    std::optional<AtomIds> getAtomIds(const ASTPtr & ast) const;
    std::vector<CNFQuery::AtomicFormula> getAtomsById(const AtomIds & ids) const;

    class QueryTreeData
    {
    public:
        const QueryTreeNodes & getConstraints() const;
        const std::vector<std::vector<Analyzer::CNF::AtomicFormula>> & getConstraintData() const;
        std::optional<AtomIds> getAtomIds(const QueryTreeNodePtrWithHash & node_with_hash) const;
        std::vector<Analyzer::CNF::AtomicFormula> getAtomsById(const AtomIds & ids) const;
        const ComparisonGraph<QueryTreeNodePtr> & getGraph() const;
    private:
        QueryTreeNodes constraints;
        std::vector<std::vector<Analyzer::CNF::AtomicFormula>> cnf_constraints;
        QueryTreeNodePtrWithHashMap<AtomIds> query_node_to_atom_ids;
        std::unique_ptr<ComparisonGraph<QueryTreeNodePtr>> graph;

        friend ConstraintsDescription;
    };

    QueryTreeData getQueryTreeData(const ContextPtr & context, const QueryTreeNodePtr & table_node) const;

private:
    std::vector<std::vector<CNFQuery::AtomicFormula>> buildConstraintData() const;
    std::unique_ptr<ComparisonGraph<ASTPtr>> buildGraph() const;
    void update();

    ASTs constraints;

    std::vector<std::vector<CNFQuery::AtomicFormula>> cnf_constraints;
    std::map<IAST::Hash, AtomIds> ast_to_atom_ids;

    std::unique_ptr<ComparisonGraph<ASTPtr>> graph;
};

}
