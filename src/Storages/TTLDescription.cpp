#include <Storages/TTLDescription.h>

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>


#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int BAD_TTL_EXPRESSION;
}

namespace
{

void checkTTLExpression(const ExpressionActionsPtr & ttl_expression, const String & result_column_name)
{
    for (const auto & action : ttl_expression->getActions())
    {
        if (action.type == ExpressionAction::APPLY_FUNCTION)
        {
            IFunctionBase & func = *action.function_base;
            if (!func.isDeterministic())
                throw Exception(
                    "TTL expression cannot contain non-deterministic functions, "
                    "but contains function "
                        + func.getName(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    const auto & result_column = ttl_expression->getSampleBlock().getByName(result_column_name);

    if (!typeid_cast<const DataTypeDateTime *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate *>(result_column.type.get()))
    {
        throw Exception(
            "TTL expression result column should have DateTime or Date type, but has " + result_column.type->getName(),
            ErrorCodes::BAD_TTL_EXPRESSION);
    }
}

}

TTLDescription TTLDescription::getTTLFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const Context & context,
    const StorageMetadataKeyField & primary_key)
{
    TTLDescription result;
    const auto * ttl_element = definition_ast->as<ASTTTLElement>();

    /// First child is expression: `TTL expr TO DISK`
    if (ttl_element != nullptr)
        result.expression_ast = ttl_element->children.front()->clone();
    else /// It's columns TTL without any additions, just copy it
        result.expression_ast = definition_ast->clone();

    auto ttl_ast = result.expression_ast->clone();
    auto syntax_analyzer_result = SyntaxAnalyzer(context).analyze(ttl_ast, columns.getAllPhysical());
    result.expression = ExpressionAnalyzer(ttl_ast, syntax_analyzer_result, context).getActions(false);
    result.result_column = ttl_ast->getColumnName();

    if (ttl_element == nullptr) /// columns TTL
    {
        result.destination_type = DataDestinationType::DELETE;
        result.mode = TTLMode::DELETE;
    }
    else /// rows TTL
    {
        result.destination_type = ttl_element->destination_type;
        result.destination_name = ttl_element->destination_name;
        result.mode = ttl_element->mode;

        if (ttl_element->mode == TTLMode::DELETE)
        {
            if (ASTPtr where_expr_ast = ttl_element->where())
            {
                auto where_syntax_result = SyntaxAnalyzer(context).analyze(where_expr_ast, columns.getAllPhysical());
                result.where_expression = ExpressionAnalyzer(where_expr_ast, where_syntax_result, context).getActions(false);
                result.where_result_column = where_expr_ast->getColumnName();
            }
        }
        else if (ttl_element->mode == TTLMode::GROUP_BY)
        {
            const auto & pk_columns = primary_key.column_names;

            if (ttl_element->group_by_key.size() > pk_columns.size())
                throw Exception("TTL Expression GROUP BY key should be a prefix of primary key", ErrorCodes::BAD_TTL_EXPRESSION);

            NameSet primary_key_columns_set(pk_columns.begin(), pk_columns.end());
            NameSet aggregation_columns_set;

            for (const auto & column : primary_key.expression->getRequiredColumns())
                primary_key_columns_set.insert(column);

            for (size_t i = 0; i < ttl_element->group_by_key.size(); ++i)
            {
                if (ttl_element->group_by_key[i]->getColumnName() != pk_columns[i])
                    throw Exception(
                        "TTL Expression GROUP BY key should be a prefix of primary key",
                        ErrorCodes::BAD_TTL_EXPRESSION);
            }

            for (const auto & [name, value] : ttl_element->group_by_aggregations)
            {
                if (primary_key_columns_set.count(name))
                    throw Exception(
                        "Can not set custom aggregation for column in primary key in TTL Expression",
                        ErrorCodes::BAD_TTL_EXPRESSION);

                aggregation_columns_set.insert(name);
            }

            if (aggregation_columns_set.size() != ttl_element->group_by_aggregations.size())
                throw Exception(
                    "Multiple aggregations set for one column in TTL Expression",
                    ErrorCodes::BAD_TTL_EXPRESSION);


            result.group_by_keys = Names(pk_columns.begin(), pk_columns.begin() + ttl_element->group_by_key.size());

            auto aggregations = ttl_element->group_by_aggregations;

            for (size_t i = 0; i < pk_columns.size(); ++i)
            {
                ASTPtr value = primary_key.expression_list_ast->children[i]->clone();

                if (i >= ttl_element->group_by_key.size())
                {
                    ASTPtr value_max = makeASTFunction("max", value->clone());
                    aggregations.emplace_back(value->getColumnName(), std::move(value_max));
                }

                if (value->as<ASTFunction>())
                {
                    auto syntax_result = SyntaxAnalyzer(context).analyze(value, columns.getAllPhysical(), {}, true);
                    auto expr_actions = ExpressionAnalyzer(value, syntax_result, context).getActions(false);
                    for (const auto & column : expr_actions->getRequiredColumns())
                    {
                        if (i < ttl_element->group_by_key.size())
                        {
                            ASTPtr expr = makeASTFunction("any", std::make_shared<ASTIdentifier>(column));
                            aggregations.emplace_back(column, std::move(expr));
                        }
                        else
                        {
                            ASTPtr expr = makeASTFunction("argMax", std::make_shared<ASTIdentifier>(column), value->clone());
                            aggregations.emplace_back(column, std::move(expr));
                        }
                    }
                }
            }

            for (const auto & column : columns.getAllPhysical())
            {
                if (!primary_key_columns_set.count(column.name) && !aggregation_columns_set.count(column.name))
                {
                    ASTPtr expr = makeASTFunction("any", std::make_shared<ASTIdentifier>(column.name));
                    aggregations.emplace_back(column.name, std::move(expr));
                }
            }

            for (auto [name, value] : aggregations)
            {
                auto syntax_result = SyntaxAnalyzer(context).analyze(value, columns.getAllPhysical(), {}, true);
                auto expr_analyzer = ExpressionAnalyzer(value, syntax_result, context);

                result.set_parts.emplace_back(TTLAggregateDescription{
                    name, value->getColumnName(), expr_analyzer.getActions(false)});

                for (const auto & descr : expr_analyzer.getAnalyzedData().aggregate_descriptions)
                    result.aggregate_descriptions.push_back(descr);
            }
        }
    }

    checkTTLExpression(result.expression, result.result_column);


    return result;
}

}
