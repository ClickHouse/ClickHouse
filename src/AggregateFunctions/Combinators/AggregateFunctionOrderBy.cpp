#include <AggregateFunctions/Combinators/AggregateFunctionOrderBy.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Columns/Collator.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
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

/// One element of the sort description: the sort key at position `i` among the trailing sort-key arguments.
struct OrderByElement
{
    int direction = 1;
    int nulls_direction = 1;
    std::shared_ptr<Collator> collator;
};

struct OrderByDescription
{
    std::vector<OrderByElement> elements; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::optional<UInt64> limit;
};

/** Parse the sort description string, which is the first parameter of the combinator.
  *
  * The string is built by the analyzer (or supplied by the user when calling the parametric form
  * directly) and looks like "0 ASC, 1 DESC NULLS FIRST COLLATE 'tr' LIMIT 10" — positional references
  * to the trailing sort-key arguments with the usual ORDER BY modifiers, followed by an optional LIMIT.
  * The positions must be exactly 0, 1, ..., N-1: the i-th element describes the i-th sort key.
  * We use the standard ParserOrderByExpressionList here so that we accept exactly the same syntax
  * that ORDER BY normally accepts.
  */
OrderByDescription parseOrderByDescription(const String & combinator_name, const Array & parameters)
{
    if (parameters.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Aggregate function with {} suffix requires at least one parameter (sort description string)", combinator_name);

    if (parameters[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The first parameter of aggregate function with {} suffix must be a string with the sort description, got {}",
            combinator_name, parameters[0].getTypeName());

    const String & sort_desc_str = parameters[0].safeGet<String>();

    Tokens tokens(sort_desc_str.data(), sort_desc_str.data() + sort_desc_str.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;

    ASTPtr sort_asts;
    ParserOrderByExpressionList parser;
    if (!parser.parse(pos, sort_asts, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "Cannot parse the sort description of aggregate function with {} suffix: '{}'", combinator_name, sort_desc_str);

    OrderByDescription result;

    if (ParserKeyword(Keyword::LIMIT).ignore(pos, expected))
    {
        ASTPtr limit_ast;
        if (!ParserUnsignedInteger().parse(pos, limit_ast, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                "Cannot parse LIMIT in the sort description of aggregate function with {} suffix: '{}'", combinator_name, sort_desc_str);
        result.limit = limit_ast->as<ASTLiteral &>().value.safeGet<UInt64>();
    }

    if (!pos->isEnd())
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "Unexpected trailing data in the sort description of aggregate function with {} suffix: '{}'", combinator_name, sort_desc_str);

    const auto & sort_list = sort_asts->as<ASTExpressionList &>();
    result.elements.reserve(sort_list.children.size());
    for (size_t i = 0; i < sort_list.children.size(); ++i)
    {
        const auto & sort_elem = sort_list.children[i]->as<ASTOrderByElement &>();

        const auto * position = sort_elem.children.front()->as<ASTLiteral>();
        if (!position || position->value.getType() != Field::Types::UInt64 || position->value.safeGet<UInt64>() != i)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Element {} of the sort description of aggregate function with {} suffix must be the position {} "
                "of the sort key among the trailing sort-key arguments, got '{}'",
                i, combinator_name, i, sort_elem.children.front()->formatForErrorMessage());

        if (sort_elem.with_fill)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "WITH FILL is not supported in the sort description of aggregate function with {} suffix", combinator_name);

        OrderByElement element;
        element.direction = sort_elem.direction;
        element.nulls_direction = sort_elem.nulls_direction;
        if (auto collation = sort_elem.getCollation())
            element.collator = std::make_shared<Collator>(collation->as<ASTLiteral &>().value.safeGet<String>());
        result.elements.push_back(std::move(element));
    }

    return result;
}


class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "OrderBy"; }

    /// The OrderBy combinator consumes trailing arguments equal to the number of sort keys.
    /// The number of sort keys is determined by parsing the first parameter (the sort description string).
    size_t getNumberOfNestedArguments(const DataTypes & arguments, const Array & parameters) const override
    {
        const size_t sort_keys_count = parseOrderByDescription(getName(), parameters).elements.size();

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

    /// Strip the sort-description parameter (which also contains the optional LIMIT)
    /// before passing the rest to the nested function.
    Array transformParameters(const Array & parameters) const override
    {
        if (parameters.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function with {} suffix requires at least one parameter (sort description string)", getName());

        return Array(parameters.begin() + 1, parameters.end());
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties & /*properties*/,
        const DataTypes & arguments,
        const Array & params) const override
    {
        /// Re-parse the sort description (we already parsed it once in getNumberOfNestedArguments,
        /// but the cost is negligible — this happens once per query during preparation).
        OrderByDescription description = parseOrderByDescription(getName(), params);

        const size_t sort_keys_count = description.elements.size();
        if (arguments.size() <= sort_keys_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Not enough arguments for {} combinator: have {}, need at least {}",
                getName(), arguments.size(), sort_keys_count + 1);

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
        for (auto & element : description.elements)
        {
            sort_description.emplace_back(std::to_string(column_idx), element.direction, element.nulls_direction, element.collator);
            ++column_idx;
        }

        return std::make_shared<AggregateFunctionOrderBy>(
            arguments, params, nested_function, sort_description, description.limit);
    }
};

}

void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>(), Documentation{
        .description = "Feeds the rows to the aggregate function in the specified order, optionally only the first N rows in that order. "
            "It is usually applied with the syntax `f(x ORDER BY y [ASC|DESC] [NULLS FIRST|LAST] [COLLATE 'locale'], ... [LIMIT N])`, "
            "which is rewritten to the parametric form `fOrderBy('0 ASC, ... LIMIT N')(x, y, ...)`: the first parameter describes "
            "the sort keys passed as the trailing arguments by their positions, the remaining parameters are passed to the nested function.",
        .syntax = "<aggregate_function>OrderBy",
        .related = {"Distinct"}});
}

}
