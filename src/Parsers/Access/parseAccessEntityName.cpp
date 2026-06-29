#include <Parsers/Access/parseAccessEntityName.h>
#include <Parsers/CommonParsers.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>

#include <algorithm>


namespace DB
{

bool atQueryOutputTail(IParser::Pos & pos, Expected & expected)
{
    return ParserKeyword{Keyword::FORMAT}.checkWithoutMoving(pos, expected)
        || ParserKeyword{Keyword::SETTINGS}.checkWithoutMoving(pos, expected)
        || ParserKeyword{Keyword::INTO_OUTFILE}.checkWithoutMoving(pos, expected)
        || ParserKeyword{Keyword::PARALLEL_WITH}.checkWithoutMoving(pos, expected);
}

String backQuoteAccessEntityNameIfNeed(const String & name)
{
    /// FORMAT and SETTINGS are the only single-token output-tail keywords; INTO OUTFILE and
    /// PARALLEL WITH are two tokens and so can never be a single bare identifier.
    auto equals_keyword = [&](Keyword keyword)
    {
        return std::ranges::equal(name, toStringView(keyword), equalsCaseInsensitive);
    };

    if (equals_keyword(Keyword::FORMAT) || equals_keyword(Keyword::SETTINGS))
        return backQuote(name);

    return backQuoteIfNeed(name);
}

}
