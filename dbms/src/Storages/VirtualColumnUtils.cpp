#include <Core/NamesAndTypes.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>

#include <Storages/VirtualColumnUtils.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace VirtualColumnUtils
{

String chooseSuffix(const NamesAndTypesList & columns, const String & name)
{
    int id = 0;
    String current_suffix;
    while (true)
    {
        bool done = true;
        for (const auto & it : columns)
            if (it.name == name + current_suffix)
            {
                done = false;
                break;
            }
        if (done) break;
        ++id;
        current_suffix = toString<Int32>(id);
    }
    return current_suffix;
}

String chooseSuffixForSet(const NamesAndTypesList & columns, const std::vector<String> & names)
{
    int id = 0;
    String current_suffix;
    while (true)
    {
        bool done = true;
        for (const auto & it : columns)
        {
            for (size_t i = 0; i < names.size(); ++i)
            {
                if (it.name == names[i] + current_suffix)
                {
                    done = false;
                    break;
                }
            }
            if (!done)
                break;
        }
        if (done)
            break;
        ++id;
        current_suffix = toString<Int32>(id);
    }
    return current_suffix;
}

void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value)
{
    ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*ast);
    if (!select.with_expression_list)
    {
        select.with_expression_list = std::make_shared<ASTExpressionList>();
        select.children.insert(select.children.begin(), select.with_expression_list);
    }

    ASTExpressionList & with = typeid_cast<ASTExpressionList &>(*select.with_expression_list);
    auto literal = std::make_shared<ASTLiteral>(value);
    literal->alias = column_name;
    literal->prefer_alias_to_column_name = true;
    with.children.push_back(literal);
}

/// Verifying that the function depends only on the specified columns
static bool isValidFunction(const ASTPtr & expression, const NameSet & columns)
{
    for (size_t i = 0; i < expression->children.size(); ++i)
        if (!isValidFunction(expression->children[i], columns))
            return false;

    if (const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(&*expression))
    {
        if (identifier->kind == ASTIdentifier::Kind::Column)
            return columns.count(identifier->name);
    }
    return true;
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
static void extractFunctions(const ASTPtr & expression, const NameSet & columns, std::vector<ASTPtr> & result)
{
    const ASTFunction * function = typeid_cast<const ASTFunction *>(expression.get());
    if (function && function->name == "and")
    {
        for (size_t i = 0; i < function->arguments->children.size(); ++i)
            extractFunctions(function->arguments->children[i], columns, result);
    }
    else if (isValidFunction(expression, columns))
    {
        result.push_back(expression->clone());
    }
}

/// Construct a conjunction from given functions
static ASTPtr buildWhereExpression(const ASTs & functions)
{
    if (functions.size() == 0)
        return nullptr;
    if (functions.size() == 1)
        return functions[0];
    ASTPtr new_query = std::make_shared<ASTFunction>();
    ASTFunction & new_function = typeid_cast<ASTFunction & >(*new_query);
    new_function.name = "and";
    new_function.arguments = std::make_shared<ASTExpressionList>();
    new_function.arguments->children = functions;
    new_function.children.push_back(new_function.arguments);
    return new_query;
}

void filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context)
{
    const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery & >(*query);
    if (!select.where_expression && !select.prewhere_expression)
        return;

    NameSet columns;
    for (const auto & it : block.getNamesAndTypesList())
        columns.insert(it.name);

    /// We will create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    std::vector<ASTPtr> functions;
    if (select.where_expression)
        extractFunctions(select.where_expression, columns, functions);
    if (select.prewhere_expression)
        extractFunctions(select.prewhere_expression, columns, functions);

    ASTPtr expression_ast = buildWhereExpression(functions);
    if (!expression_ast)
        return;

    /// Let's analyze and calculate the expression.
    ExpressionAnalyzer analyzer(expression_ast, context, {}, block.getNamesAndTypesList());
    ExpressionActionsPtr actions = analyzer.getActions(false);

    Block block_with_filter = block;
    actions->execute(block_with_filter);

    /// Filter the block.
    String filter_column_name = expression_ast->getColumnName();
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column;
    if (ColumnPtr converted = filter_column->convertToFullColumnIfConst())
        filter_column = converted;
    const IColumn::Filter & filter = typeid_cast<const ColumnUInt8 &>(*filter_column).getData();

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(filter, -1);
    }
}

}

}
