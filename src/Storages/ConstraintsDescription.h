#pragma once

#include <Parsers/ASTConstraintDeclaration.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>

namespace DB
{

using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

struct ConstraintsDescription
{
public:
    ConstraintsDescription() { update(); }

    bool empty() const { return constraints.empty(); }
    String toString() const;

    static ConstraintsDescription parse(const String & str);

    enum class ConstraintType
    {
        CHECK = 1,
        ASSUME = 2,
        ALWAYS_TRUE = CHECK | ASSUME,
        ALL = CHECK | ASSUME,
    };

    ASTs filterConstraints(ConstraintType selection) const;

    const std::vector<ASTPtr> & getConstraints() const;
    void updateConstraints(const std::vector<ASTPtr> & constraints);

    const std::vector<std::vector<CNFQuery::AtomicFormula>> & getConstraintData() const;
    std::vector<CNFQuery::AtomicFormula> getAtomicConstraintData() const;

    const ComparisonGraph & getGraph() const;

    ConstraintsExpressions getExpressions(ContextPtr context, const NamesAndTypesList & source_columns_) const;

    ConstraintsDescription(const ConstraintsDescription & other);
    ConstraintsDescription & operator=(const ConstraintsDescription & other);

private:
    std::vector<std::vector<CNFQuery::AtomicFormula>> buildConstraintData() const;
    std::unique_ptr<ComparisonGraph> buildGraph() const;
    void update();

    std::vector<ASTPtr> constraints;
    std::vector<std::vector<CNFQuery::AtomicFormula>> cnf_constraints;
    std::unique_ptr<ComparisonGraph> graph;
};

}
