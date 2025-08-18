#include "parseIdentifierOrStringLiteral.h"

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        ASTPtr ast;
        if (ParserIdentifier().parse(pos, ast, expected))
        {
            result = getIdentifierName(ast);
            return true;
        }

        if (ParserStringLiteral().parse(pos, ast, expected))
        {
            result = ast->as<ASTLiteral &>().value.safeGet<String>();
            return !result.empty();
        }

        return false;
    });
}


bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result)
{
    Strings res;

    auto parse_single_id_or_literal = [&]
    {
        String str;
        if (!parseIdentifierOrStringLiteral(pos, expected, str))
            return false;

        res.emplace_back(std::move(str));
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_single_id_or_literal, false))
        return false;

    result = std::move(res);
    return true;
}

std::vector<String> parseIdentifiersOrStringLiterals(const String & str, const Settings & settings)
{
    Tokens tokens(str.data(), str.data() + str.size(), settings[Setting::max_query_size]);
    IParser::Pos pos(tokens, static_cast<unsigned>(settings[Setting::max_parser_depth]), static_cast<unsigned>(settings[Setting::max_parser_backtracks]));

    Expected expected;
    std::vector<String> res;

    if (!parseIdentifiersOrStringLiterals(pos, expected, res))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse string ('{}') into vector of identifiers", str);

    return res;
}

}
