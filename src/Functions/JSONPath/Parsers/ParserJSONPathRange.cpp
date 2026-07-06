#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathQuery.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathRange.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
/**
 *
 * @param pos token iterator
 * @param node node of ASTJSONPathQuery
 * @param expected stuff for logging
 * @return was parse successful
 */
bool ParserJSONPathRange::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{

    if (pos->type != TokenType::OpeningSquareBracket)
    {
        return false;
    }
    ++pos;

    auto range = std::make_shared<ASTJSONPathRange>();
    node = range;

    ParserNumber number_p;
    ASTPtr number_ptr;
    while (pos->type != TokenType::ClosingSquareBracket)
    {
        if (pos->type != TokenType::Number)
        {
            return false;
        }

        std::pair<UInt32, UInt32> range_indices;
        if (!number_p.parse(pos, number_ptr, expected))
        {
            return false;
        }
        range_indices.first = static_cast<UInt32>(number_ptr->as<ASTLiteral>()->value.safeGet<UInt32>());

        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingSquareBracket)
        {
            /// Single index case
            range_indices.second = range_indices.first + 1;
        }
        else if (pos->type == TokenType::BareWord)
        {
            if (!ParserKeyword(Keyword::TO).ignore(pos, expected))
            {
                return false;
            }
            if (!number_p.parse(pos, number_ptr, expected))
            {
                return false;
            }
            range_indices.second = static_cast<UInt32>(number_ptr->as<ASTLiteral>()->value.safeGet<UInt32>());
        }
        else
        {
            return false;
        }

        if (range_indices.first >= range_indices.second)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Start of range must be greater than end of range, however {} >= {}",
                range_indices.first,
                range_indices.second);
        }

        range->ranges.push_back(std::move(range_indices));
        if (pos->type != TokenType::ClosingSquareBracket)
        {
            ++pos;
        }
    }
    ++pos;

    /// We can't have both ranges and star present, so parse was successful <=> exactly 1 of these conditions is true
    return !range->ranges.empty() ^ range->is_star;
}

}
