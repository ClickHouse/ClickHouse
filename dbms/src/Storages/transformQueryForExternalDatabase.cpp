#include <sstream>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/transformQueryForExternalDatabase.h>


namespace DB
{

Block getBlockWithConstants(
    const ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns)
{
    Block result
    {
        { DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }
    };

    const auto expr_for_constant_folding = ExpressionAnalyzer{query, context, nullptr, all_columns}.getConstActions();

    expr_for_constant_folding->execute(result);

    return result;
}

static void replaceConstFunction(const IAST & node, const Context & context, const NamesAndTypesList & all_columns)
{
    for (size_t i = 0; i < node.children.size(); ++i)
    {
        auto child = node.children[i];
        if (const ASTExpressionList * exp_list = typeid_cast<const ASTExpressionList *>(&*child))
            replaceConstFunction(*exp_list, context, all_columns);

        if (const ASTFunction * function = typeid_cast<const ASTFunction *>(&*child))
        {
            auto result_block = getBlockWithConstants(function->clone(), context, all_columns);
            if (!result_block.has(child->getColumnName()))
                return;

            auto result_column = result_block.getByName(child->getColumnName()).column;
            auto result_string = applyVisitor(FieldVisitorToString(), (*result_column)[0]);

            auto new_node = const_cast<IAST *>(&node);

            new_node->children[i] = std::make_shared<ASTLiteral>((*result_column)[0]);
        }
    }
}

static bool isCompatible(const IAST & node)
{
    if (const ASTFunction * function = typeid_cast<const ASTFunction *>(&node))
    {
        String name = function->name;
        if (!(name == "and"
            || name == "or"
            || name == "not"
            || name == "equals"
            || name == "notEquals"
            || name == "like"
            || name == "notLike"
            || name == "in"
            || name == "greater"
            || name == "less"
            || name == "lessOrEquals"
            || name == "greaterOrEquals"))
            return false;

        for (const auto & expr : function->arguments->children)
            if (!isCompatible(*expr.get()))
                return false;

        return true;
    }

    if (const ASTLiteral * literal = typeid_cast<const ASTLiteral *>(&node))
    {
        /// Foreign databases often have no support for Array and Tuple literals.
        if (literal->value.getType() == Field::Types::Array
            || literal->value.getType() == Field::Types::Tuple)
            return false;

        return true;
    }

    if (typeid_cast<const ASTIdentifier *>(&node))
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
    ExpressionAnalyzer analyzer(clone_query, context, {}, available_columns);
    const Names & used_columns = analyzer.getRequiredSourceColumns();

    auto select = std::make_shared<ASTSelectQuery>();

    select->replaceDatabaseAndTable(database, table);

    auto select_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & name : used_columns)
        select_expr_list->children.push_back(std::make_shared<ASTIdentifier>(name));

    select->select_expression_list = std::move(select_expr_list);

    /** If there was WHERE,
      * copy it to transformed query if it is compatible,
      * or if it is AND expression,
      * copy only compatible parts of it.
      */

    const ASTPtr & original_where = typeid_cast<const ASTSelectQuery &>(*clone_query).where_expression;
    if (original_where)
    {
        replaceConstFunction(*original_where, context, available_columns);
        if (isCompatible(*original_where))
        {
            select->where_expression = original_where;
        }
        else if (const ASTFunction * function = typeid_cast<const ASTFunction *>(original_where.get()))
        {
            if (function->name == "and")
            {
                auto new_function_and = std::make_shared<ASTFunction>();
                auto new_function_and_arguments = std::make_shared<ASTExpressionList>();
                new_function_and->arguments = new_function_and_arguments;
                new_function_and->children.push_back(new_function_and_arguments);

                for (const auto & elem : function->arguments->children)
                    if (isCompatible(*elem))
                        new_function_and_arguments->children.push_back(elem);

                select->where_expression = std::move(new_function_and);
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
