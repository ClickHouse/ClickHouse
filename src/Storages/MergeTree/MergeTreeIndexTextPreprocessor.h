#pragma once

#include <Interpreters/ExpressionActions.h>

namespace DB
{

struct ColumnWithTypeAndName;
struct IndexDescription;

class MergeTreeIndexTextPreprocessor
{
public:
    MergeTreeIndexTextPreprocessor(const String & expression, const IndexDescription & index_description);

    /// Processes n_rows rows of the column in index_column_with_type_and_name starting at start_row.
    /// The transformation is only applied in the range [start_row, start_row + n_rows)
    /// If the expression is empty this functions is just a no-op.
    /// Returns a pair with the result column and the starting position where results were written.
    /// If the expression is empty this just returns the input column and start_row.
    /// The input column is not modified.
    std::pair<ColumnPtr, size_t> processColumn(const ColumnWithTypeAndName & index_column_with_type_and_name, size_t start_row, size_t n_rows) const;

    /// Applies the modification expression to an input string.
    /// This is somehow equivalent to: SELECT expression(input)
    String process(const String & input) const;

    /// This function parses an string to build an ExpressionActions.
    /// The conversion is not direct and requires many steps and validations, but long story short
    /// ParserExpression(String) => AST; ActionsVisitor(AST) => ActionsDAG; ExpressionActions(ActionsDAG)
    static ExpressionActions parseExpression(const IndexDescription & index, const String & expression);
private:
    ExpressionActions expression;
    DataTypePtr column_type;
    String column_name;
};

}
