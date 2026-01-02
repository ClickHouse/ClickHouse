#include "AggregateFunctionOrderBy.h"

#include "AggregateFunctionCombinatorFactory.h"

#include <AggregateFunctions/Helpers.h>

#include "Core/SortDescription.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTOrderByElement.h"
#include "Parsers/ExpressionListParsers.h"
#include "Parsers/IParser.h"
#include "Parsers/TokenIterator.h"

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NUMBER_OF_PARAMETERS_DOESNT_MATCH;
    extern const int SYNTAX_ERROR;
}

namespace
{

class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
{
public:
    static size_t getArgumentsNumber(const String & parameter) {
        size_t result = 0;
        for (auto c : parameter) {
            if (c == ',') {
                ++result;
            }
        }
        return result + 1;
    }

    String getName() const override { return "OrderBy"; }

    DataTypes transformArguments(const DataTypes & arguments, const Array & parameters) const override
    {
        if (parameters.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect number of arguments for aggregate function with {} suffix", getName());
        auto arguments_number = getArgumentsNumber(parameters[0].safeGet<String>());

        if (arguments.size() <= arguments_number)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect number of arguments for aggregate function with {} suffix", getName());

        return {arguments.begin(), arguments.end() - arguments_number};
    }

    Array transformParameters(const Array & parameters) const override {
        return {parameters.begin() + 1, parameters.end()};
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        auto sort_desc_str = params[0].safeGet<String>();
        Tokens tokens(sort_desc_str.data(), sort_desc_str.data() + sort_desc_str.size());

        IParser::Pos pos(tokens, 10, 10);
        ASTPtr sort_asts = nullptr;
        Expected expected;

        ParserOrderByExpressionList parser;
        parser.parse(pos, sort_asts, expected);

        if (!std::dynamic_pointer_cast<ASTExpressionList>(sort_asts))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse ORDER BY expression list");

        SortDescription sorts;

        size_t column_idx = arguments.size() - sort_asts->children.size();
        for (const auto & child : sort_asts->children) {
            auto sort_ast = std::dynamic_pointer_cast<ASTOrderByElement>(child);
            if (!sort_ast)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse ORDER BY expression list");

            SortColumnDescription description{
                std::to_string(column_idx++),
                sort_ast->direction,
                sort_ast->nulls_direction,
                nullptr,
                sort_ast->with_fill
            };

            sorts.emplace_back(std::move(description));
        }

        return std::make_shared<AggregateFunctionOrderBy>(arguments, params, nested_function, sorts);
    }
};

}

void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>());
}

}
