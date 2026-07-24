#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/FieldFromAST.h>

#include <Core/Names.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class ParameterFieldVisitorToString : public StaticVisitor<String>
{
public:
    template <class T>
    String operator() (const T & x) const
    {
        FieldVisitorToString visitor;
        return visitor(x);
    }

    String operator() (const Array & x) const
    {
        WriteBufferFromOwnString wb;

        wb << '[';
        for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb.write(", ", 2);
            wb << applyVisitor(*this, *it);
        }
        wb << ']';

        return wb.str();
    }

    String operator() (const Map & x) const
    {
        WriteBufferFromOwnString wb;

        wb << '{';

        auto it = x.begin();
        while (it != x.end())
        {
            if (it != x.begin())
                wb << ", ";
            wb << applyVisitor(*this, *it);
            ++it;

            if (it != x.end())
            {
                wb << ':';
                wb << applyVisitor(*this, *it);
                ++it;
            }
        }
        wb << '}';

        return wb.str();
    }

    String operator() (const Tuple & x) const
    {
        WriteBufferFromOwnString wb;

        wb << '(';
        for (auto it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb << ", ";
            wb << applyVisitor(*this, *it);
        }
        wb << ')';

        return wb.str();
    }
};


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

/// Parse Identifier, Literal, Array/Tuple/Map of literals
bool parseParameterValueIntoString(IParser::Pos & pos, String & value, Expected & expected)
{
    ASTPtr node;

    /// 1. Identifier
    ParserCompoundIdentifier identifier_p;

    if (identifier_p.parse(pos, node, expected))
    {
        tryGetIdentifierNameInto(node, value);
        return true;
    }

    /// 2. Literal
    ParserLiteral literal_p;
    if (literal_p.parse(pos, node, expected))
    {
        value = applyVisitor(FieldVisitorToString(), node->as<ASTLiteral>()->value);

        /// writeQuoted is not always quoted in line with SQL standard https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/WriteHelpers.h
        if (value.starts_with('\''))
        {
            ReadBufferFromOwnString buf(value);
            readQuoted(value, buf);
        }

        return true;
    }

    /// 3. Map, Array, Tuple of literals and their combination
    ParserAllCollectionsOfLiterals all_collections_p;

    if (all_collections_p.parse(pos, node, expected))
    {
        value = applyVisitor(ParameterFieldVisitorToString(), node->as<ASTLiteral>()->value);
        return true;
    }

    return false;
}

/// Parse `name = value`.
bool ParserSetQuery::parseNameValuePair(SettingChange & change, IParser::Pos & pos, Expected & expected)
{
    ParserCompoundIdentifier name_p;
    ParserLiteralOrMap literal_or_map_p;
    ParserToken s_eq(TokenType::Equals);
    ParserSetQuery set_p(true);
    ParserFunction function_p;

    ASTPtr name;
    ASTPtr value;
    ASTPtr function_ast;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    /// for SETTINGS disk=disk(type='s3', path='', ...)
    if (function_p.parse(pos, function_ast, expected) && function_ast->as<ASTFunction>()->name == "disk")
    {
        tryGetIdentifierNameInto(name, change.name);
        change.value = createFieldFromAST(function_ast);

        return true;
    }
    if (!literal_or_map_p.parse(pos, value, expected))
        return false;

    tryGetIdentifierNameInto(name, change.name);
    change.value = value->as<ASTLiteral &>().value;

    return true;
}

bool ParserSetQuery::parseNameValuePairWithParameterOrDefault(
    SettingChange & change, String & default_settings, ParserSetQuery::Parameter & parameter, IParser::Pos & pos, Expected & expected)
{
    ParserCompoundIdentifier name_p;
    ParserLiteralOrMap value_p;
    ParserToken s_eq(TokenType::Equals);
    ParserFunction function_p;

    ASTPtr node;
    String name;
    ASTPtr function_ast;

    if (!name_p.parse(pos, node, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    tryGetIdentifierNameInto(node, name);

    /// Parameter
    if (name.starts_with(QUERY_PARAMETER_NAME_PREFIX))
    {
        name = name.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter name cannot be empty");

        String value;

        if (!parseParameterValueIntoString(pos, value, expected))
            return false;

        parameter = {std::move(name), std::move(value)};
        return true;
    }

    /// Default
    if (ParserKeyword(Keyword::DEFAULT).ignore(pos, expected))
    {
        default_settings = name;
        return true;
    }

    /// Setting
    if (function_p.parse(pos, function_ast, expected) && function_ast->as<ASTFunction>()->name == "disk")
    {
        change.name = name;
        change.value = createFieldFromAST(function_ast);

        return true;
    }
    if (!value_p.parse(pos, node, expected))
        return false;

    change.name = name;
    change.value = node->as<ASTLiteral &>().value;

    return true;
}


bool ParserSetQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    if (!parse_only_internals)
    {
        ParserKeyword s_set(Keyword::SET);

        if (!s_set.ignore(pos, expected))
            return false;

        /// Parse SET TRANSACTION ... queries using ParserTransactionControl
        if (ParserKeyword{Keyword::TRANSACTION}.check(pos, expected))
            return false;
    }

    SettingsChanges changes;
    NameToNameVector query_parameters;
    std::vector<String> default_settings;

    while (true)
    {
        if ((!changes.empty() || !query_parameters.empty() || !default_settings.empty()) && !s_comma.ignore(pos))
            break;

        SettingChange setting;
        String name_of_default_setting;
        Parameter parameter;

        if (!parseNameValuePairWithParameterOrDefault(setting, name_of_default_setting, parameter, pos, expected))
            return false;

        if (!parameter.first.empty())
            query_parameters.emplace_back(std::move(parameter));
        else if (!name_of_default_setting.empty())
            default_settings.emplace_back(std::move(name_of_default_setting));
        else
            changes.push_back(std::move(setting));
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
