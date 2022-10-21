#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>

#include <Core/Names.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class ParserLiteralOrMap : public IParserBase
{
public:
protected:
    const char * getName() const override { return "literal or map"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        {
            ParserLiteral literal;
            if (literal.parse(pos, node, expected))
                return true;
        }

        ParserToken l_br(TokenType::OpeningCurlyBrace);
        ParserToken r_br(TokenType::ClosingCurlyBrace);
        ParserToken comma(TokenType::Comma);
        ParserToken colon(TokenType::Colon);
        ParserStringLiteral literal;

        if (!l_br.ignore(pos, expected))
            return false;

        Map map;

        while (!r_br.ignore(pos, expected))
        {
            if (!map.empty() && !comma.ignore(pos, expected))
                return false;

            ASTPtr key;
            ASTPtr val;

            if (!literal.parse(pos, key, expected))
                return false;

            if (!colon.ignore(pos, expected))
                return false;

            if (!literal.parse(pos, val, expected))
                return false;

            Tuple tuple;
            tuple.push_back(std::move(key->as<ASTLiteral>()->value));
            tuple.push_back(std::move(val->as<ASTLiteral>()->value));
            map.push_back(std::move(tuple));
        }

        node = std::make_shared<ASTLiteral>(std::move(map));
        return true;
    }
};

/// Parse `param_name = value`
/// Compared to the usual setting, parameter can be
/// Literal, Array and Tuple of literals, Identifier, Map
bool ParserSetQuery::parseNameValuePairParameter(ParserSetQuery::Parameter & change, IParser::Pos & pos, Expected & expected)
{
    ParserLiteral literal_p;
    ParserArrayOfLiterals array_p;
    ParserTupleOfLiterals tuple_p;
    ParserCompoundIdentifier identifier_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name, value;
    String name_str, value_str;

    if (!identifier_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    tryGetIdentifierNameInto(name, name_str);

    if (!name_str.starts_with(QUERY_PARAMETER_NAME_PREFIX))
        return false;

    name_str = name_str.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));
    if (name_str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter name cannot be empty");

    if (literal_p.parse(pos, value, expected)
        || array_p.parse(pos, value, expected)
        || tuple_p.parse(pos, value, expected))
    {

        value_str = applyVisitor(FieldVisitorToString(), value->as<ASTLiteral>()->value);

        /// writeQuoted is not always quoted in line with SQL standard https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/WriteHelpers.h
        if (value_str.starts_with('\''))
        {
            ReadBufferFromOwnString buf(value_str);
            readQuoted(value_str, buf);
        }
    }
    else if (identifier_p.parse(pos, value, expected))
    {
        tryGetIdentifierNameInto(value, value_str);
    }
    else
    {
        ParserToken l_br_p(TokenType::OpeningCurlyBrace);
        ParserToken r_br_p(TokenType::ClosingCurlyBrace);
        ParserToken comma_p(TokenType::Comma);
        ParserToken colon_p(TokenType::Colon);

        if (!l_br_p.ignore(pos, expected))
            return false;

        int depth = 1;

        value_str = '{';

        while (depth > 0)
        {
            if (r_br_p.ignore(pos, expected))
            {
                value_str += '}';
                --depth;
                continue;
            }

            if (value_str.back() != '{')
            {
                if (!comma_p.ignore(pos, expected))
                    return false;

                value_str += ',';
            }

            ASTPtr key;
            ASTPtr val;

            if (!literal_p.parse(pos, key, expected))
                return false;

            if (!colon_p.ignore(pos, expected))
                return false;

            value_str += applyVisitor(FieldVisitorToString(), key->as<ASTLiteral>()->value);
            value_str += ":";

            if (l_br_p.ignore(pos, expected))
            {
                value_str += '{';
                ++depth;
                continue;
            }

            if (!literal_p.parse(pos, val, expected)
                && !array_p.parse(pos, val, expected)
                && !tuple_p.parse(pos, val, expected))
                return false;

            value_str += applyVisitor(FieldVisitorToString(), val->as<ASTLiteral>()->value);
        }
    }

    change = {std::move(name_str), std::move(value_str)};
    return true;
}

/// Parse `name = value`.
bool ParserSetQuery::parseNameValuePair(SettingChange & change, IParser::Pos & pos, Expected & expected)
{
    ParserCompoundIdentifier name_p;
    ParserLiteralOrMap value_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name;
    ASTPtr value;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    if (ParserKeyword("TRUE").ignore(pos, expected))
        value = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
    else if (ParserKeyword("FALSE").ignore(pos, expected))
        value = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(0)));
    else if (!value_p.parse(pos, value, expected))
        return false;

    tryGetIdentifierNameInto(name, change.name);
    change.value = value->as<ASTLiteral &>().value;

    return true;
}

bool ParserSetQuery::parseNameValuePairWithDefault(SettingChange & change, String & default_settings, IParser::Pos & pos, Expected & expected)
{
    ParserCompoundIdentifier name_p;
    ParserLiteralOrMap value_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name;
    ASTPtr value;
    bool is_default = false;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    if (ParserKeyword("TRUE").ignore(pos, expected))
        value = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
    else if (ParserKeyword("FALSE").ignore(pos, expected))
        value = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(0)));
    else if (ParserKeyword("DEFAULT").ignore(pos, expected))
        is_default = true;
    else if (!value_p.parse(pos, value, expected))
        return false;

    tryGetIdentifierNameInto(name, change.name);
    if (is_default)
        default_settings = change.name;
    else
        change.value = value->as<ASTLiteral &>().value;

    return true;
}


bool ParserSetQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    if (!parse_only_internals)
    {
        ParserKeyword s_set("SET");

        if (!s_set.ignore(pos, expected))
            return false;

        /// Parse SET TRANSACTION ... queries using ParserTransactionControl
        if (ParserKeyword{"TRANSACTION"}.check(pos, expected))
            return false;
    }

    SettingsChanges changes;
    NameToNameMap query_parameters;
    std::vector<String> default_settings;

    while (true)
    {
        if ((!changes.empty() || !query_parameters.empty() || !default_settings.empty()) && !s_comma.ignore(pos))
            break;

        auto old_pos = pos;
        Parameter parameter;

        if (parseNameValuePairParameter(parameter, pos, expected))
        {
            query_parameters.emplace(std::move(parameter));
            continue;
        }

        pos = old_pos;

        SettingChange setting;
        String name_of_default_setting;

        if (!parseNameValuePairWithDefault(current, name_of_default_setting, pos, expected))
            return false;

        if (!name_of_default_setting.empty())
            default_settings.emplace_back(std::move(name_of_default_setting));
        else
            changes.push_back(std::move(current));
    }

    auto query = std::make_shared<ASTSetQuery>();
    node = query;

    query->is_standalone = !parse_only_internals;
    query->changes = std::move(changes);
    query->query_parameters = std::move(query_parameters);
    query->default_settings = std::move(default_settings);

    return true;
}


}
