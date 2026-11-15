#include <Interpreters/parseIdentifiersOrStringLiteralsWithSettings.h>

#include <Core/Settings.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Common/Exception.h>

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

std::unordered_set<String> parseIdentifiersOrStringLiteralsToSet(const String & str, const Settings & settings)
{
    Tokens tokens(str.data(), str.data() + str.size(), settings[Setting::max_query_size]);
    IParser::Pos pos(
        tokens,
        static_cast<unsigned>(settings[Setting::max_parser_depth]),
        static_cast<unsigned>(settings[Setting::max_parser_backtracks]));

    Expected expected;
    std::unordered_set<std::string> res;

    auto parse_single_id_or_literal = [&]
    {
        String str_out;
        if (!parseIdentifierOrStringLiteral(pos, expected, str_out))
            return false;

        res.insert(std::move(str_out));
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_single_id_or_literal, false))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse string ('{}') into set of identifiers", str);

    return res;
}

}
