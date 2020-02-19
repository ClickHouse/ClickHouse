#include "evaluateMissingDefaults.h"

#include <Core/Block.h>
#include <Storages/ColumnDefault.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <utility>
#include <DataTypes/DataTypesNumber.h>


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
        {
            auto cast_func = makeASTFunction("CAST", it->second.expression->clone(), std::make_shared<ASTLiteral>(column.type->getName()));
            default_expr_list->children.emplace_back(setAlias(cast_func, it->first));
        }
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
        auto syntax_result = SyntaxAnalyzer(context).analyze(default_expr_list, block.getNamesAndTypesList());
        ExpressionAnalyzer{default_expr_list, syntax_result, context}.getActions(true)->execute(block);
        return;
    }

    /** ExpressionAnalyzer eliminates "unused" columns, in order to ensure their safety
      * we are going to operate on a copy instead of the original block */
    Block copy_block{block};

    auto syntax_result = SyntaxAnalyzer(context).analyze(default_expr_list, block.getNamesAndTypesList());
    auto expression_analyzer = ExpressionAnalyzer{default_expr_list, syntax_result, context};
    auto required_source_columns = syntax_result->requiredSourceColumns();
    auto rows_was = copy_block.rows();

    // Delete all not needed columns in DEFAULT expression.
    // They can intersect with columns added in PREWHERE
    // test 00950_default_prewhere
    // CLICKHOUSE-4523
    for (const auto & delete_column : copy_block.getNamesAndTypesList())
    {
        if (std::find(required_source_columns.begin(), required_source_columns.end(), delete_column.name) == required_source_columns.end())
        {
            copy_block.erase(delete_column.name);
        }
    }

    if (copy_block.columns() == 0)
    {
        // Add column to indicate block size in execute()
        copy_block.insert({DataTypeUInt8().createColumnConst(rows_was, 0u), std::make_shared<DataTypeUInt8>(), "__dummy"});
    }

    expression_analyzer.getActions(true)->execute(copy_block);

    /// move evaluated columns to the original block, materializing them at the same time
    size_t pos = 0;
    for (auto col = required_columns.begin(); col != required_columns.end(); ++col, ++pos)
    {
        if (copy_block.has(col->name))
        {
            auto evaluated_col = copy_block.getByName(col->name);
            evaluated_col.column = evaluated_col.column->convertToFullColumnIfConst();

            block.insert(pos, std::move(evaluated_col));
        }
    }
}

}
