#include <Core/NamesAndTypes.h>

#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

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
    for (const auto & child : expression->children)
        if (!isValidFunction(child, columns))
            return false;

    if (auto opt_name = IdentifierSemantic::getColumnName(expression))
        return columns.count(*opt_name);

    return true;
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
void extractFunctions(const ASTPtr & expression, const NameSet & columns, std::vector<ASTPtr> & result)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            extractFunctions(child, columns, result);
    }
    else if (isValidFunction(expression, columns))
    {
        result.push_back(expression->clone());
    }
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

void filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where() && !select.prewhere())
        return;

    NameSet columns;
    for (const auto & it : block.getNamesAndTypesList())
        columns.insert(it.name);

    /// We will create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    std::vector<ASTPtr> functions;
    if (select.where())
        extractFunctions(select.where(), columns, functions);
    if (select.prewhere())
        extractFunctions(select.prewhere(), columns, functions);

    ASTPtr expression_ast = buildWhereExpression(functions);
    if (!expression_ast)
        return;

    /// Let's analyze and calculate the expression.
    auto syntax_result = TreeRewriter(context).analyze(expression_ast, block.getNamesAndTypesList());
    ExpressionAnalyzer analyzer(expression_ast, syntax_result, context);
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
