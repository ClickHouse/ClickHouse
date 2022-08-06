#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Core/Names.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Common/typeid_cast.h>

using namespace DB;

namespace
{
NameToNameMap::value_type convertToQueryParameter(SettingChange change)
{
    auto name = change.name.substr(6);
    auto value = applyVisitor(FieldVisitorToString(), change.value);
    /// writeQuoted is not always quoted in line with SQL standard https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/WriteHelpers.h
    if (value.starts_with('\''))
    {
        ReadBufferFromOwnString buf(value);
        readQuoted(value, buf);
    }
    return {name, value};
}
}


namespace DB
{

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

    while (true)
    {
        if ((!changes.empty() || !query_parameters.empty()) && !s_comma.ignore(pos))
            break;

        /// Either a setting or a parameter for prepared statement (if name starts with "param_")
        SettingChange current;

        if (!parseNameValuePair(current, pos, expected))
            return false;

        if (current.name.starts_with("param_"))
            query_parameters.emplace(convertToQueryParameter(std::move(current)));
        else
            changes.push_back(std::move(current));
    }

    auto query = std::make_shared<ASTSetQuery>();
    node = query;

    query->is_standalone = !parse_only_internals;
    query->changes = std::move(changes);
    query->query_parameters = std::move(query_parameters);

    return true;
}


}
