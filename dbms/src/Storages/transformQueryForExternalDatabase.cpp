#include <sstream>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MergeTree/KeyCondition.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void replaceConstFunction(IAST & node, const Context & context, const NamesAndTypesList & all_columns)
{
    for (size_t i = 0; i < node.children.size(); ++i)
    {
        auto child = node.children[i];
        if (auto * exp_list = child->as<ASTExpressionList>())
            replaceConstFunction(*exp_list, context, all_columns);

        if (auto * function = child->as<ASTFunction>())
        {
            NamesAndTypesList source_columns = all_columns;
            ASTPtr query = function->ptr();
            auto syntax_result = SyntaxAnalyzer(context).analyze(query, source_columns);
            auto result_block = KeyCondition::getBlockWithConstants(query, syntax_result, context);
            if (!result_block.has(child->getColumnName()))
                return;

            auto result_column = result_block.getByName(child->getColumnName()).column;

            node.children[i] = std::make_shared<ASTLiteral>((*result_column)[0]);
        }
    }
}

static bool isCompatible(const IAST & node)
{
    if (const auto * function = node.as<ASTFunction>())
    {
        if (function->parameters)   /// Parametric aggregate functions
            return false;

        if (!function->arguments)
            throw Exception("Logical error: function->arguments is not set", ErrorCodes::LOGICAL_ERROR);

        String name = function->name;

        if (!(name == "and"
            || name == "or"
            || name == "not"
            || name == "equals"
            || name == "notEquals"
            || name == "less"
            || name == "greater"
            || name == "lessOrEquals"
            || name == "greaterOrEquals"
            || name == "like"
            || name == "notLike"
            || name == "in"
            || name == "notIn"
            || name == "tuple"))
            return false;

        /// A tuple with zero or one elements is represented by a function tuple(x) and is not compatible,
        /// but a normal tuple with more than one element is represented as a parenthesed expression (x, y) and is perfectly compatible.
        if (name == "tuple" && function->arguments->children.size() <= 1)
            return false;

        for (const auto & expr : function->arguments->children)
            if (!isCompatible(*expr.get()))
                return false;

        return true;
    }

    if (const auto * literal = node.as<ASTLiteral>())
    {
        /// Foreign databases often have no support for Array. But Tuple literals are passed to support IN clause.
        if (literal->value.getType() == Field::Types::Array)
            return false;

        return true;
    }

    if (node.as<ASTIdentifier>())
        return true;

    return false;
}


String transformQueryForExternalDatabase(
    const IAST & query,
    const NamesAndTypesList & available_columns,
    IdentifierQuotingStyle identifier_quoting_style,
    const String & database,
    const String & table,
    const Context & context)
{
    auto clone_query = query.clone();
    auto syntax_result = SyntaxAnalyzer(context).analyze(clone_query, available_columns);
    const Names used_columns = syntax_result->requiredSourceColumns();

    auto select = std::make_shared<ASTSelectQuery>();

    select->replaceDatabaseAndTable(database, table);

    auto select_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & name : used_columns)
        select_expr_list->children.push_back(std::make_shared<ASTIdentifier>(name));

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

    /** If there was WHERE,
      * copy it to transformed query if it is compatible,
      * or if it is AND expression,
      * copy only compatible parts of it.
      */

    ASTPtr original_where = clone_query->as<ASTSelectQuery &>().where();
    if (original_where)
    {
        replaceConstFunction(*original_where, context, available_columns);
        if (isCompatible(*original_where))
        {
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(original_where));
        }
        else if (const auto * function = original_where->as<ASTFunction>())
        {
            if (function->name == "and")
            {
                bool compatible_found = false;
                auto new_function_and = makeASTFunction("and");
                for (const auto & elem : function->arguments->children)
                {
                    if (isCompatible(*elem))
                    {
                        new_function_and->arguments->children.push_back(elem);
                        compatible_found = true;
                    }
                }
                if (new_function_and->arguments->children.size() == 1)
                    new_function_and->name = "";

                if (compatible_found)
                    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_function_and));
            }
        }
    }

    std::stringstream out;
    IAST::FormatSettings settings(out, true);
    settings.always_quote_identifiers = true;
    settings.identifier_quoting_style = identifier_quoting_style;

    select->format(settings);

    return out.str();
}

}
