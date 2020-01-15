#include "evaluateMissingDefaults.h"

#include <iostream>
#include <Core/Block.h>
#include <Parsers/queryToString.h>
#include <Storages/ColumnDefault.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <utility>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace {
ASTPtr defaultRequiredExpressions(Block & block, const NamesAndTypesList & required_columns, const ColumnDefaults & column_defaults)
{
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : required_columns)
    {
        if (block.has(column.name))
            continue;

        const auto it = column_defaults.find(column.name);

        /// expressions must be cloned to prevent modification by the ExpressionAnalyzer
        if (it != column_defaults.end())
            default_expr_list->children.emplace_back(setAlias(it->second.expression->clone(), it->first));
    }

    if (default_expr_list->children.empty())
        return nullptr;
    return default_expr_list;
}

ASTPtr convertRequiredExpressions(Block & block, const NamesAndTypesList & required_columns)
{
    ASTPtr conversion_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & required_column : required_columns)
    {
        if (!block.has(required_column.name))
            continue;
            //throw Exception("Required conversion of column " + required_column.name + " which is absent in block. It's a bug", ErrorCodes::LOGICAL_ERROR);

        auto column_in_block = block.getByName(required_column.name);
        //std::cerr << "Looking at:" << required_column.name << std::endl;
        //std::cerr << "In block type:" << column_in_block.type->getName() << std::endl;
        //std::cerr << "Required type:" << required_column.type->getName() << std::endl;
        if (column_in_block.type->equals(*required_column.type))
        {
            //std::cerr << "TYPES ARE SAME\n";
            continue;
        }
        //std::cerr << "TYPES ARE DIFFERENT\n";

        auto cast_func = makeASTFunction(
            "CAST", std::make_shared<ASTIdentifier>(required_column.name), std::make_shared<ASTLiteral>(required_column.type->getName()));

        conversion_expr_list->children.emplace_back(setAlias(cast_func, required_column.name));

    }
    return conversion_expr_list;
}

void executeExpressionsOnBlock(
    Block & block,
    ASTPtr expr_list,
    bool save_unneeded_columns,
    const NamesAndTypesList & required_columns,
    const Context & context)
{
    if (!expr_list)
        return;

    if (!save_unneeded_columns)
    {
        auto syntax_result = SyntaxAnalyzer(context).analyze(expr_list, block.getNamesAndTypesList());
        ExpressionAnalyzer{expr_list, syntax_result, context}.getActions(true)->execute(block);
        return;
    }

    /** ExpressionAnalyzer eliminates "unused" columns, in order to ensure their safety
      * we are going to operate on a copy instead of the original block */
    Block copy_block{block};

    auto syntax_result = SyntaxAnalyzer(context).analyze(expr_list, block.getNamesAndTypesList());
    auto expression_analyzer = ExpressionAnalyzer{expr_list, syntax_result, context};
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

    //std::cerr << "Block before expression:" << copy_block.dumpStructure() << std::endl;
    expression_analyzer.getActions(true)->execute(copy_block);
    //std::cerr << "Block after expression:" << copy_block.dumpStructure() << std::endl;

    /// move evaluated columns to the original block, materializing them at the same time
    size_t pos = 0;
    for (auto col = required_columns.begin(); col != required_columns.end(); ++col, ++pos)
    {
        if (copy_block.has(col->name))
        {
            auto evaluated_col = copy_block.getByName(col->name);
            evaluated_col.column = evaluated_col.column->convertToFullColumnIfConst();

            if (block.has(col->name))
                block.getByName(col->name) = std::move(evaluated_col);
            else
                block.insert(pos, std::move(evaluated_col));
        }
    }
}

}

void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, const Context & context)
{
    ASTPtr conversion_expr_list = convertRequiredExpressions(block, required_columns);
    //std::cerr << queryToString(conversion_expr_list) << std::endl;
    //std::cerr << "Block:" << block.dumpStructure() << std::endl;
    if (conversion_expr_list->children.empty())
        return;
    executeExpressionsOnBlock(block, conversion_expr_list, true, required_columns, context);
}

void evaluateMissingDefaults(Block & block,
    const NamesAndTypesList & required_columns,
    const ColumnDefaults & column_defaults,
    const Context & context, bool save_unneeded_columns)
{
    if (column_defaults.empty())
        return;

    ASTPtr default_expr_list = defaultRequiredExpressions(block, required_columns, column_defaults);
    executeExpressionsOnBlock(block, default_expr_list, save_unneeded_columns, required_columns, context);
}

}
