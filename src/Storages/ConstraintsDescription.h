#pragma once

#include <Parsers/ASTConstraintDeclaration.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

using ConstraintsASTs = std::vector<ASTPtr>;
using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

struct ConstraintsDescription
{
    ConstraintsASTs constraints;

    ConstraintsDescription() = default;

    bool empty() const { return constraints.empty(); }
    String toString() const;

    static ConstraintsDescription parse(const String & str);

    ConstraintsExpressions getExpressions(const Context & context, const NamesAndTypesList & source_columns_) const;
};

}
