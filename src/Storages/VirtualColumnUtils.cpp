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


namespace DB
{

namespace
{

/// Verifying that the function depends only on the specified columns
bool isValidFunction(const ASTPtr & expression, const NameSet & columns)
{
    const auto * function = expression->as<ASTFunction>();
    if (function)
    {
        if (functionIsInOrGlobalInOperator(function->name))
        {
            // Second argument of IN can be a scalar subquery
            if (!isValidFunction(function->arguments->children[0], columns))
                return false;
        }
        else if (function->name == "ignore")
        {
            return false;
        }
        else
        {
            if (function->arguments)
            {
                for (const auto & child : function->arguments->children)
                    if (!isValidFunction(child, columns))
                        return false;
            }
        }
    }
    else
    {
        if (auto opt_name = IdentifierSemantic::getColumnName(expression))
            return columns.count(*opt_name);
    }

    return true;
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
bool extractFunctions(const ASTPtr & expression, const NameSet & columns, std::vector<ASTPtr> & result)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && (function->name == "and" || function->name == "indexHint"))
    {
        bool ret = true;
        for (const auto & child : function->arguments->children)
            ret &= extractFunctions(child, columns, result);
        return ret;
    }
    else if (isValidFunction(expression, columns))
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

bool prepareFilterBlockWithQuery(const ASTPtr & query, const Block & block, ASTPtr & expression_ast)
{
    bool ret = true;
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where() && !select.prewhere())
        return ret;

    NameSet columns;
    for (const auto & it : block.getNamesAndTypesList())
        columns.insert(it.name);

    /// We will create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    std::vector<ASTPtr> functions;
    if (select.where())
        ret &= extractFunctions(select.where(), columns, functions);
    if (select.prewhere())
        ret &= extractFunctions(select.prewhere(), columns, functions);

    expression_ast = buildWhereExpression(functions);
    return ret;
}

void filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context, ASTPtr expression_ast)
{
    if (!expression_ast)
        prepareFilterBlockWithQuery(query, block, expression_ast);

    if (!expression_ast)
        return;

    /// Let's analyze and calculate the expression.
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
