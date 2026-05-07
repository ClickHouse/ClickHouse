#include <AggregateFunctions/Combinators/AggregateFunctionOrderBy.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
}

namespace
{

/** Parse the sort description string back into an AST.
  *
  * The string is built by the analyzer (or supplied by the user when calling the parametric form
  * directly) and looks like "0 ASC, 1 DESC NULLS FIRST" — column-index references with sort
  * direction and nulls-direction modifiers. We use the standard ParserOrderByExpressionList here so
  * that we accept exactly the same syntax that ORDER BY normally accepts.
  */
ASTPtr parseSortDescriptionString(const String & sort_desc_str)
{
    Tokens tokens(sort_desc_str.data(), sort_desc_str.data() + sort_desc_str.size());
    IParser::Pos pos(tokens, 0, 0);
    ASTPtr sort_asts;
    Expected expected;
    ParserOrderByExpressionList parser;
    if (!parser.parse(pos, sort_asts, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "Cannot parse ORDER BY expression list inside aggregate function combinator: '{}'", sort_desc_str);
    return sort_asts;
}


class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "OrderBy"; }

    /// The OrderBy combinator consumes trailing arguments equal to the number of sort keys.
    /// The number of sort keys is determined by parsing the first parameter (the sort description string).
    size_t getNumberOfNestedArguments(const DataTypes & arguments, const Array & parameters) const override
    {
        if (parameters.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function with {} suffix requires at least one parameter (sort description string)", getName());

        const String & sort_desc_str = parameters[0].safeGet<String>();
        ASTPtr sort_asts = parseSortDescriptionString(sort_desc_str);

        const auto * sort_list = sort_asts->as<ASTExpressionList>();
        if (!sort_list)
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                "Expected expression list in ORDER BY description for {} combinator", getName());

        const size_t sort_keys_count = sort_list->children.size();

        if (arguments.size() <= sort_keys_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function with {} suffix expects at least {} arguments (got {}): "
                "trailing {} arguments are sort keys, the rest are passed to the nested function",
                getName(), sort_keys_count + 1, arguments.size(), sort_keys_count);

        return arguments.size() - sort_keys_count;
    }

    /// transformArguments is called by the factory AFTER it has already sliced off the sort keys
    /// (using the count returned by getNumberOfNestedArguments). So `arguments` here are exactly
    /// the nested-function arguments — pass them through unchanged.
    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        return arguments;
    }

    /// Strip the sort-description parameter (and the optional LIMIT parameter, if present)
    /// before passing the rest to the nested function.
    Array transformParameters(const Array & parameters) const override
    {
        if (parameters.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function with {} suffix requires at least one parameter", getName());

        size_t consumed = 1;
        /// If a second parameter is present and is an unsigned integer, treat it as LIMIT.
        if (parameters.size() >= 2 && parameters[1].getType() == Field::Types::UInt64)
            consumed = 2;
        return Array(parameters.begin() + consumed, parameters.end());
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties & /*properties*/,
        const DataTypes & arguments,
        const Array & params) const override
    {
        if (params.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function with {} suffix requires at least one parameter", getName());

        /// Sort description: first parameter, must be a String.
        const String sort_desc_str = params[0].safeGet<String>();

        /// Optional LIMIT: second parameter, if it's UInt64.
        std::optional<UInt64> limit;
        if (params.size() >= 2 && params[1].getType() == Field::Types::UInt64)
            limit = params[1].safeGet<UInt64>();

        /// Re-parse the sort description (we already parsed it once in getNumberOfNestedArguments,
        /// but the cost is negligible — this happens once per query during preparation).
        ASTPtr sort_asts = parseSortDescriptionString(sort_desc_str);
        const auto * sort_list = sort_asts->as<ASTExpressionList>();
        if (!sort_list)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected expression list in ORDER BY description");

        const size_t sort_keys_count = sort_list->children.size();
        if (arguments.size() < sort_keys_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Not enough arguments for {} combinator: have {}, need at least {} (sort keys)",
                getName(), arguments.size(), sort_keys_count);

        /// Build the SortDescription. The columns being sorted are the LAST sort_keys_count
        /// columns of `arguments` (which here is the FULL argument list, not the sliced one —
        /// this is by interface convention: transformAggregateFunction sees the original arguments).
        ///
        /// We name sort columns by their integer index, matching what AggregateFunctionOrderBy
        /// does in insertResultInto when it builds the Block: column at position i has name
        /// std::to_string(i).
        SortDescription sort_description;
        sort_description.reserve(sort_keys_count);

        size_t column_idx = arguments.size() - sort_keys_count;
        for (const auto & child : sort_list->children)
        {
            const auto * sort_elem = child->as<ASTOrderByElement>();
            if (!sort_elem)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected ORDER BY element in sort description");

            sort_description.emplace_back(SortColumnDescription{
                std::to_string(column_idx),
                sort_elem->direction,
                sort_elem->nulls_direction,
                nullptr,
                sort_elem->with_fill
            });
            ++column_idx;
        }

        return std::make_shared<AggregateFunctionOrderBy>(
            arguments, params, nested_function, sort_description, limit);
    }
};

}

void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>());
}

}
