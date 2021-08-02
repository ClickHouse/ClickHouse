#include "inplaceBlockConversions.h"

#include <Core/Block.h>
#include <Parsers/queryToString.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <utility>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Common/checkStackSize.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

namespace
{

/// Add all required expressions for missing columns calculation
void addDefaultRequiredExpressionsRecursively(const Block & block, const String & required_column, const ColumnsDescription & columns, ASTPtr default_expr_list_accum, NameSet & added_columns)
{
    checkStackSize();
    if (block.has(required_column) || added_columns.count(required_column))
        return;

    auto column_default = columns.getDefault(required_column);

    if (column_default)
    {
        /// expressions must be cloned to prevent modification by the ExpressionAnalyzer
        auto column_default_expr = column_default->expression->clone();

        /// Our default may depend on columns with default expr which not present in block
        /// we have to add them to block too
        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(column_default_expr);
        NameSet required_columns_names = columns_context.requiredColumns();

        auto cast_func = makeASTFunction("CAST", column_default_expr, std::make_shared<ASTLiteral>(columns.get(required_column).type->getName()));
        default_expr_list_accum->children.emplace_back(setAlias(cast_func, required_column));
        added_columns.emplace(required_column);

        for (const auto & required_column_name : required_columns_names)
            addDefaultRequiredExpressionsRecursively(block, required_column_name, columns, default_expr_list_accum, added_columns);
    }
}

ASTPtr defaultRequiredExpressions(const Block & block, const NamesAndTypesList & required_columns, const ColumnsDescription & columns)
{
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    NameSet added_columns;
    for (const auto & column : required_columns)
        addDefaultRequiredExpressionsRecursively(block, column.name, columns, default_expr_list, added_columns);

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

        auto column_in_block = block.getByName(required_column.name);
        if (column_in_block.type->equals(*required_column.type))
            continue;

        auto cast_func = makeASTFunction(
            "CAST", std::make_shared<ASTIdentifier>(required_column.name), std::make_shared<ASTLiteral>(required_column.type->getName()));

        conversion_expr_list->children.emplace_back(setAlias(cast_func, required_column.name));

    }
    return conversion_expr_list;
}

ActionsDAGPtr createExpressions(
    const Block & header,
    ASTPtr expr_list,
    bool save_unneeded_columns,
    const NamesAndTypesList & required_columns,
    ContextPtr context)
{
    if (!expr_list)
        return nullptr;

    auto syntax_result = TreeRewriter(context).analyze(expr_list, header.getNamesAndTypesList());
    auto expression_analyzer = ExpressionAnalyzer{expr_list, syntax_result, context};
    auto dag = std::make_shared<ActionsDAG>(header.getNamesAndTypesList());
    auto actions = expression_analyzer.getActionsDAG(true, !save_unneeded_columns);
    dag = ActionsDAG::merge(std::move(*dag), std::move(*actions));

    if (save_unneeded_columns)
    {
        dag->removeUnusedActions(required_columns.getNames());
        dag->addMaterializingOutputActions();
    }

    return dag;
}

}

void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, ContextPtr context)
{
    ASTPtr conversion_expr_list = convertRequiredExpressions(block, required_columns);
    if (conversion_expr_list->children.empty())
        return;

    if (auto dag = createExpressions(block, conversion_expr_list, true, required_columns, context))
    {
        auto expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings::fromContext(context));
        expression->execute(block);
    }
}

ActionsDAGPtr evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context, bool save_unneeded_columns)
{
    if (!columns.hasDefaults())
        return nullptr;

    ASTPtr expr_list = defaultRequiredExpressions(header, required_columns, columns);
    return createExpressions(header, expr_list, save_unneeded_columns, required_columns, context);
}

}
