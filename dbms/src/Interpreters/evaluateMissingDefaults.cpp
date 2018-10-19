#include <Core/Block.h>
#include <Storages/ColumnDefault.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <utility>


namespace DB
{

void evaluateMissingDefaults(Block & block,
    const NamesAndTypesList & required_columns,
    const ColumnDefaults & column_defaults,
    const Context & context)
{
    if (column_defaults.empty())
        return;

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : required_columns)
    {
        if (block.has(column.name))
            continue;

        const auto it = column_defaults.find(column.name);

        /// expressions must be cloned to prevent modification by the ExpressionAnalyzer
        if (it != column_defaults.end())
            default_expr_list->children.emplace_back(
                setAlias(it->second.expression->clone(), it->first));
    }

    /// nothing to evaluate
    if (default_expr_list->children.empty())
        return;

    /** ExpressionAnalyzer eliminates "unused" columns, in order to ensure their safety
      * we are going to operate on a copy instead of the original block */
    Block copy_block{block};
    /// evaluate default values for defaulted columns

    NamesAndTypesList available_columns;
    for (size_t i = 0, size = block.columns(); i < size; ++i)
        available_columns.emplace_back(block.getByPosition(i).name, block.getByPosition(i).type);

    ExpressionAnalyzer{default_expr_list, context, {}, available_columns}.getActions(true)->execute(copy_block);

    /// move evaluated columns to the original block, materializing them at the same time
    size_t pos = 0;
    for (auto col = required_columns.begin(); col != required_columns.end(); ++col, ++pos)
    {
        if (copy_block.has(col->name))
        {
            auto evaluated_col = copy_block.getByName(col->name);
            if (ColumnPtr converted = evaluated_col.column->convertToFullColumnIfConst())
                evaluated_col.column = converted;

            block.insert(pos, std::move(evaluated_col));
        }
    }
}

}
