#include <Core/Block.h>
#include <Storages/ColumnDefault.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <utility>


namespace DB
{

static ASTPtr requiredExpressions(Block & block, const NamesAndTypesList & required_columns, const ColumnDefaults & column_defaults)
{
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

    if (default_expr_list->children.empty())
        return nullptr;
    return default_expr_list;
}

void evaluateMissingDefaults(Block & block,
    const NamesAndTypesList & required_columns,
    const ColumnDefaults & column_defaults,
    const Context & context, bool save_unneeded_columns)
{
    if (column_defaults.empty())
        return;

    ASTPtr default_expr_list = requiredExpressions(block, required_columns, column_defaults);
    if (!default_expr_list)
        return;

    if (!save_unneeded_columns)
    {
        auto syntax_result = SyntaxAnalyzer(context, {}).analyze(default_expr_list, block.getNamesAndTypesList());
        ExpressionAnalyzer{default_expr_list, syntax_result, context}.getActions(true)->execute(block);
        return;
    }

    /** ExpressionAnalyzer eliminates "unused" columns, in order to ensure their safety
      * we are going to operate on a copy instead of the original block */
    Block copy_block{block};

    auto syntax_result = SyntaxAnalyzer(context, {}).analyze(default_expr_list, block.getNamesAndTypesList());
    ExpressionAnalyzer{default_expr_list, syntax_result, context}.getActions(true)->execute(copy_block);

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
