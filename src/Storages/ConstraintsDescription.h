#pragma once

#include <Parsers/ASTConstraintDeclaration.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeCNFConverter.h>

namespace DB
{

using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

struct ConstraintsDescription
{
    std::vector<ASTPtr> constraints;
    std::vector<CNFQuery> cnf_constraints;

    ConstraintsDescription() = default;

    bool empty() const { return constraints.empty(); }
    String toString() const;

    static ConstraintsDescription parse(const String & str);

    enum class ConstraintType {
        CHECK = 1,
        ASSUME = 2,
        ALWAYS_TRUE = CHECK | ASSUME,
        ALL = CHECK | ASSUME,
    };

    ASTs filterConstraints(ConstraintType selection) const;
    // TODO: перенести преобразование в КНФ + get constraitns
    //ASTs filterAtomicConstraints(ConstraintType selection) const;
    //ASTs filterEqualConstraints(ConstraintType selection) const;

    ConstraintsExpressions getExpressionsToCheck(const Context & context, const NamesAndTypesList & source_columns_) const;

    ConstraintsDescription(const ConstraintsDescription & other);
    ConstraintsDescription & operator=(const ConstraintsDescription & other);
};

}
