#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct ColumnWithTypeAndName;
struct IndexDescription;

class MergeTreeIndexTextPreprocessor
{
public:
    MergeTreeIndexTextPreprocessor(ASTPtr expression_ast, const IndexDescription & index_description);

    /// Processes n_rows rows of input column, starting at start_row.
    /// The transformation is only applied in the range [start_row, start_row + n_rows)
    /// If the expression is empty this functions is just a no-op.
    /// Returns a pair with the result column and the starting position where results were written.
    std::pair<ColumnPtr, size_t> processColumn(const ColumnWithTypeAndName & column, size_t start_row, size_t n_rows) const;

    /// Applies the preprocessor expression to a constant string.
    String processConstant(const String & input) const;

    bool hasActions() const { return !original_actions.getActions().empty(); }
    const ActionsDAG & getOriginalActionsDAG() const { return original_actions.getActionsDAG(); }

private:
    /// The name of the column on which the index is defined.
    String index_column_name;
    /// The type of the column on which the index is defined.
    DataTypePtr index_column_type;
    /// The original expression actions that executes the preprocessor expression
    /// and the index expression from the required index column.
    ExpressionActions original_actions;
    /// The expression actions that executes the preprocessor expression on top the ready index column.
    ExpressionActions actions_for_index_column;
    /// The expression actions that executes the preprocessor expression on top the constant string with needles.
    ExpressionActions actions_for_constant;
};

}
