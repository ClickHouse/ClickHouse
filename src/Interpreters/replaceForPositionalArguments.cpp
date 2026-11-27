#include <Interpreters/replaceForPositionalArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

bool replaceForPositionalArguments(ASTPtr & argument, const ASTSelectQuery * select_query, ASTSelectQuery::Expression expression)
{
    auto columns = select_query->select()->children;

    const auto * expr_with_alias = dynamic_cast<const ASTWithAlias *>(argument.get());

    if (expr_with_alias && !expr_with_alias->alias.empty())
        return false;

    const auto * ast_literal = typeid_cast<const ASTLiteral *>(argument.get());
    if (!ast_literal)
        return false;

    auto which = ast_literal->value.getType();
    if (which != Field::Types::UInt64 && which != Field::Types::Int64)
        return false;

    UInt64 pos;

    if (which == Field::Types::UInt64)
    {
        pos = ast_literal->value.safeGet<UInt64>();
    }
    else if (which == Field::Types::Int64)
    {
        auto value = ast_literal->value.safeGet<Int64>();
        if (value > 0)
            pos = value;
        else
        {
            if (value < -static_cast<Int64>(columns.size()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Negative positional argument number {} is out of bounds. Expected in range [-{}, -1]",
                    value,
                    columns.size());
            pos = columns.size() + value + 1;
        }
    }
    else
    {
        return false;
    }

    if (!pos || pos > columns.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional argument out of bounds: {} (expected in range [1, {}]", pos, columns.size());

    const auto & column = columns[--pos];
    if (typeid_cast<const ASTIdentifier *>(column.get()) || typeid_cast<const ASTLiteral *>(column.get()))
    {
        argument = column->clone();
    }
    else if (typeid_cast<const ASTFunction *>(column.get()))
    {
        std::function<void(ASTPtr)> throw_if_aggregate_function = [&](ASTPtr node)
        {
            if (const auto * function = typeid_cast<const ASTFunction *>(node.get()))
            {
                auto is_aggregate_function = AggregateUtils::isAggregateFunction(*function);
                if (is_aggregate_function)
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal value (aggregate function) for positional argument in {}",
                                    ASTSelectQuery::expressionToString(expression));
                }

                if (function->arguments)
                {
                    for (const auto & arg : function->arguments->children)
                        throw_if_aggregate_function(arg);
                }
            }
        };

        if (expression == ASTSelectQuery::Expression::GROUP_BY)
            throw_if_aggregate_function(column);

        argument = column->clone();
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal value for positional argument in {}",
                        ASTSelectQuery::expressionToString(expression));
    }

    return true;
}

}
