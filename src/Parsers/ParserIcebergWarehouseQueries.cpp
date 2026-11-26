#include <Parsers/ParserIcebergWarehouseQueries.h>

#include <Parsers/ASTCreateIcebergWarehouseQuery.h>
#include <Parsers/ASTDropIcebergWarehouseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSetQuery.h>


namespace DB
{

namespace
{

/// name = value [, name = value ...]
bool parseSettingsList(SettingsChanges & changes, IParser::Pos & pos, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);
    while (true)
    {
        if (!changes.empty() && !s_comma.ignore(pos))
            break;

        changes.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
            return false;
    }
    return true;
}

}

bool ParserCreateIcebergWarehouseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_iceberg_warehouse(Keyword::ICEBERG_WAREHOUSE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_iceberg_warehouse.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTCreateIcebergWarehouseQuery>();

    if (s_if_not_exists.ignore(pos, expected))
        query->if_not_exists = true;

    ASTPtr name;
    if (!name_p.parse(pos, name, expected))
        return false;
    query->warehouse_name = getIdentifierName(name);

    if (!s_settings.ignore(pos, expected))
        return false;

    if (!parseSettingsList(query->changes, pos, expected))
        return false;

    node = query;
    return true;
}

bool ParserDropIcebergWarehouseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_iceberg_warehouse(Keyword::ICEBERG_WAREHOUSE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_iceberg_warehouse.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTDropIcebergWarehouseQuery>();

    if (s_if_exists.ignore(pos, expected))
        query->if_exists = true;

    ASTPtr name;
    if (!name_p.parse(pos, name, expected))
        return false;
    query->warehouse_name = getIdentifierName(name);

    node = query;
    return true;
}

}
