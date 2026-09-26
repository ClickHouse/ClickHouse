#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSnapshotQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSnapshotQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}
namespace
{
using Element = ASTSnapshotQuery::Element;
using ElementType = ASTSnapshotQuery::ElementType;

bool parseExceptDatabases(IParser::Pos & pos, Expected & expected, std::set<String> & except_databases)
{
    return IParserBase::wrapParseImpl(
        pos,
        [&]
        {
            if (!ParserKeyword(Keyword::EXCEPT_DATABASE).ignore(pos, expected)
                && !ParserKeyword(Keyword::EXCEPT_DATABASES).ignore(pos, expected))
                return false;

            std::set<String> result;
            auto parse_list_element = [&]
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                result.insert(getIdentifierName(ast));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_databases = std::move(result);
            return true;
        });
}

bool parseExceptTables(
    IParser::Pos & pos, Expected & expected, const std::optional<String> & database_name, std::set<DatabaseAndTableName> & except_tables)
{
    return IParserBase::wrapParseImpl(
        pos,
        [&]
        {
            if (!ParserKeyword(Keyword::EXCEPT_TABLE).ignore(pos, expected) && !ParserKeyword(Keyword::EXCEPT_TABLES).ignore(pos, expected))
                return false;

            std::set<DatabaseAndTableName> result;
            auto parse_list_element = [&]
            {
                DatabaseAndTableName table_name;

                if (!parseDatabaseAndTableName(pos, expected, table_name.first, table_name.second))
                    return false;

                if (database_name && table_name.first.empty())
                    table_name.first = *database_name;

                if (database_name && table_name.first != *database_name)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "Database name in EXCEPT TABLES clause doesn't match the database name in DATABASE clause: {} != {}",
                        table_name.first,
                        *database_name);

                result.emplace(std::move(table_name));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_tables = std::move(result);
            return true;
        });
}

bool parseElement(IParser::Pos & pos, Expected & expected, Element & element)
{
    return IParserBase::wrapParseImpl(
        pos,
        [&]
        {
            if (ParserKeyword(Keyword::TABLE).ignore(pos, expected))
            {
                element.type = ElementType::TABLE;
                if (!parseDatabaseAndTableName(pos, expected, element.database_name, element.table_name))
                    return false;

                return true;
            }
            if (ParserKeyword(Keyword::ALL).ignore(pos, expected))
            {
                element.type = ElementType::ALL;
                parseExceptDatabases(pos, expected, element.except_databases);
                parseExceptTables(pos, expected, {}, element.except_tables);
                return true;
            }
            return false;
        });
}

bool parseSnapshotDestination(IParser::Pos & pos, Expected & expected, ASTPtr & snapshot_destination)
{
    if (!ParserIdentifierWithOptionalParameters{}.parse(pos, snapshot_destination, expected))
        return false;

    snapshot_destination->as<ASTFunction &>().setKind(ASTFunction::Kind::BACKUP_NAME);
    return true;
}
}

bool ParserSnapshotQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword(Keyword::SNAPSHOT).ignore(pos, expected))
        return false;

    Element element;
    if (!parseElement(pos, expected, element))
        return false;

    if (!ParserKeyword(Keyword::TO).ignore(pos, expected))
        return false;

    ASTPtr snapshot_destination;
    if (!parseSnapshotDestination(pos, expected, snapshot_destination))
        return false;

    auto query = make_intrusive<ASTSnapshotQuery>();
    node = query;

    query->element = std::move(element);

    if (snapshot_destination)
        query->set(query->snapshot_destination, snapshot_destination);

    return true;
}

}
