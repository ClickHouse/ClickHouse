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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>

#include <Storages/VirtualColumnUtils.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ActionsVisitor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Verifying that the function depends only on the specified columns
bool isValidFunction(const ASTPtr & expression, const std::function<bool(const ASTPtr &)> & is_constant)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && functionIsInOrGlobalInOperator(function->name))
    {
        // Second argument of IN can be a scalar subquery
        return isValidFunction(function->arguments->children.front(), is_constant);
    }
    else
        return is_constant(expression);
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
bool extractFunctions(const ASTPtr & expression, const std::function<bool(const ASTPtr &)> & is_constant, ASTList & result)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && (function->name == "and" || function->name == "indexHint"))
    {
        bool ret = true;
        for (const auto & child : function->arguments->children)
            ret &= extractFunctions(child, is_constant, result);
        return ret;
    }
    else if (isValidFunction(expression, is_constant))
    {
        result.push_back(expression->clone());
        return true;
    }
    else
        return false;
}

/// Construct a conjunction from given functions
ASTPtr buildWhereExpression(const ASTList & functions)
{
    if (functions.empty())
        return nullptr;
    if (functions.size() == 1)
        return functions.front();
    return makeASTFunction("and", functions);
}

void buildSets(const ASTPtr & expression, ExpressionAnalyzer & analyzer)
{
    const auto * func = expression->as<ASTFunction>();
    if (func && functionIsInOrGlobalInOperator(func->name))
    {
        const IAST & args = *func->arguments;
        const ASTPtr & arg = args.children.back();
        if (arg->as<ASTSubquery>() || arg->as<ASTTableIdentifier>())
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

bool prepareFilterBlockWithQuery(const ASTPtr & query, ContextPtr context, Block block, ASTPtr & expression_ast)
{
    if (block.rows() == 0)
        throw Exception("Cannot prepare filter with empty block", ErrorCodes::LOGICAL_ERROR);

    /// Take the first row of the input block to build a constant block
    auto columns = block.getColumns();
    Columns const_columns(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (isColumnConst(*columns[i]))
            const_columns[i] = columns[i]->cloneResized(1);
        else
            const_columns[i] = ColumnConst::create(columns[i]->cloneResized(1), 1);
    }
    block.setColumns(const_columns);

    bool unmodified = true;
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where() && !select.prewhere())
        return unmodified;

    // Provide input columns as constant columns to check if an expression is constant.
    std::function<bool(const ASTPtr &)> is_constant = [&block, &context](const ASTPtr & node)
    {
        auto actions = std::make_shared<ActionsDAG>(block.getColumnsWithTypeAndName());
        PreparedSets prepared_sets;
        SubqueriesForSets subqueries_for_sets;
        const NamesAndTypesList source_columns;
        const NamesAndTypesList aggregation_keys;
        const ColumnNumbersList grouping_set_keys;

        ActionsVisitor::Data visitor_data(
            context, SizeLimits{}, 1, source_columns, std::move(actions), prepared_sets, subqueries_for_sets, true, true, true, false,
            { aggregation_keys, grouping_set_keys, GroupByKind::NONE });
        ActionsVisitor(visitor_data).visit(node);
        actions = visitor_data.getActions();
        auto expression_actions = std::make_shared<ExpressionActions>(actions);
        auto block_with_constants = block;
        expression_actions->execute(block_with_constants);
        auto column_name = node->getColumnName();
        return block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column);
    };

    /// Create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    ASTList functions;
    if (select.where())
        unmodified &= extractFunctions(select.where(), is_constant, functions);
    if (select.prewhere())
        unmodified &= extractFunctions(select.prewhere(), is_constant, functions);

    expression_ast = buildWhereExpression(functions);
    return unmodified;
}

void filterBlockWithQuery(const ASTPtr & query, Block & block, ContextPtr context, ASTPtr expression_ast)
{
    if (block.rows() == 0)
        return;

    if (!expression_ast)
        prepareFilterBlockWithQuery(query, context, block, expression_ast);

    if (!expression_ast)
        return;

    /// Let's analyze and calculate the prepared expression.
    auto syntax_result = TreeRewriter(context).analyze(expression_ast, block.getNamesAndTypesList());
    ExpressionAnalyzer analyzer(expression_ast, syntax_result, context);
    buildSets(expression_ast, analyzer);
    ExpressionActionsPtr actions = analyzer.getActions(false /* add alises */, true /* project result */, CompileExpressions::yes);

    Block block_with_filter = block;
    actions->execute(block_with_filter);

    /// Filter the block.
    String filter_column_name = expression_ast->getColumnName();
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column->convertToFullColumnIfConst();

    ConstantFilterDescription constant_filter(*filter_column);

    if (constant_filter.always_true)
    {
        return;
    }

    if (constant_filter.always_false)
    {
        block = block.cloneEmpty();
        return;
    }

    FilterDescription filter(*filter_column);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }
}

}

}
