#pragma once

#include <Parsers/ASTConstraintDeclaration.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

using ConstraintsASTs = std::vector<std::shared_ptr<ASTConstraintDeclaration>>;
using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

struct ConstraintsDescription
{
    ConstraintsASTs constraints;

    ConstraintsDescription() = default;

    bool empty() const { return constraints.empty(); }
    String toString() const;

    static ConstraintsDescription parse(const String & str);

    ConstraintsExpressions getExpressions(const Context & context, const NamesAndTypesList & source_columns_) const {
        ConstraintsExpressions res;
        res.reserve(constraints.size());
        for (const auto & constraint : constraints) {
            ASTPtr expr = constraint->expr->clone();
            auto syntax_result = SyntaxAnalyzer(context).analyze(expr, source_columns_);
            res.push_back(ExpressionAnalyzer(constraint->expr->clone(), syntax_result, context).getActions(false));
        }
        return res;
    }
};

}
