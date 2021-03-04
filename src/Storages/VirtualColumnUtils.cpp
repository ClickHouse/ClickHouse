#include <Core/NamesAndTypes.h>

#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/misc.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>

#include <Storages/VirtualColumnUtils.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ActionsVisitor.h>


namespace DB
{

namespace
{

/// Verifying that the function depends only on the specified columns
bool isValidFunction(const ASTPtr & expression, const Block & block)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && functionIsInOrGlobalInOperator(function->name))
    {
        // Second argument of IN can be a scalar subquery
        return isValidFunction(function->arguments->children[0], block);
    }
    else
    {
        auto column_name = expression->getColumnName();
        return block.has(column_name) && isColumnConst(*block.getByName(column_name).column);
    }
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
bool extractFunctions(const ASTPtr & expression, const Block & block, std::vector<ASTPtr> & result)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && (function->name == "and" || function->name == "indexHint"))
    {
        bool ret = true;
        for (const auto & child : function->arguments->children)
            ret &= extractFunctions(child, block, result);
        return ret;
    }
    else if (isValidFunction(expression, block))
    {
        result.push_back(expression->clone());
        return true;
    }
    else
        return false;
}

/// Construct a conjunction from given functions
ASTPtr buildWhereExpression(const ASTs & functions)
{
    if (functions.empty())
        return nullptr;
    if (functions.size() == 1)
        return functions[0];
    return makeASTFunction("and", functions);
}

void buildSets(const ASTPtr & expression, ExpressionAnalyzer & analyzer)
{
    const auto * func = expression->as<ASTFunction>();
    if (func && functionIsInOrGlobalInOperator(func->name))
    {
        const IAST & args = *func->arguments;
        const ASTPtr & arg = args.children.at(1);
        if (arg->as<ASTSubquery>() || arg->as<ASTIdentifier>())
        {
            analyzer.tryMakeSetForIndexFromSubquery(arg);
        }
    }
    else
    {
        for (const auto & child : expression->children)
            buildSets(child, analyzer);
    }
}

}

namespace VirtualColumnUtils
{

void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value, const String & func)
{
    auto & select = ast->as<ASTSelectQuery &>();
    if (!select.with())
        select.setExpression(ASTSelectQuery::Expression::WITH, std::make_shared<ASTExpressionList>());

    if (func.empty())
    {
        auto literal = std::make_shared<ASTLiteral>(value);
        literal->alias = column_name;
        literal->prefer_alias_to_column_name = true;
        select.with()->children.push_back(literal);
    }
    else
    {
        auto literal = std::make_shared<ASTLiteral>(value);
        literal->prefer_alias_to_column_name = true;

        auto function = makeASTFunction(func, literal);
        function->alias = column_name;
        function->prefer_alias_to_column_name = true;
        select.with()->children.push_back(function);
    }
}

bool prepareFilterBlockWithQuery(const ASTPtr & query, const Context & context, Block block, ASTPtr & expression_ast)
{
    bool ret = true;
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where() && !select.prewhere())
        return ret;

    ASTPtr condition_ast;
    if (select.prewhere() && select.where())
        condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
    else
        condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();

    // Prepare a block with valid expressions
    for (size_t i = 0; i < block.columns(); ++i)
        block.getByPosition(i).column = block.getByPosition(i).type->createColumnConstWithDefaultValue(1);

    auto actions = std::make_shared<ActionsDAG>(block.getColumnsWithTypeAndName());
    PreparedSets prepared_sets;
    SubqueriesForSets subqueries_for_sets;
    ActionsVisitor::Data visitor_data(
        context, SizeLimits{}, 1, {}, std::move(actions), prepared_sets, subqueries_for_sets, true, true, true, false);
    ActionsVisitor(visitor_data).visit(condition_ast);
    actions = visitor_data.getActions();
    auto expression_actions = std::make_shared<ExpressionActions>(actions);
    expression_actions->execute(block);

    /// We will create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    std::vector<ASTPtr> functions;
    if (select.where())
        ret &= extractFunctions(select.where(), block, functions);
    if (select.prewhere())
        ret &= extractFunctions(select.prewhere(), block, functions);

    expression_ast = buildWhereExpression(functions);
    return ret;
}

void filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context, ASTPtr expression_ast)
{
    if (!expression_ast)
        prepareFilterBlockWithQuery(query, context, block, expression_ast);

    if (!expression_ast)
        return;

    /// Let's analyze and calculate the prepared expression.
    auto syntax_result = TreeRewriter(context).analyze(expression_ast, block.getNamesAndTypesList());
    ExpressionAnalyzer analyzer(expression_ast, syntax_result, context);
    buildSets(expression_ast, analyzer);
    ExpressionActionsPtr actions = analyzer.getActions(false);

    Block block_with_filter = block;
    actions->execute(block_with_filter);

    /// Filter the block.
    String filter_column_name = expression_ast->getColumnName();
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column->convertToFullColumnIfConst();

    ConstantFilterDescription constant_filter(*filter_column);

    if (constant_filter.always_true)
        return;

    if (constant_filter.always_false)
        block = block.cloneEmpty();

    FilterDescription filter(*filter_column);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }
}

}

}
