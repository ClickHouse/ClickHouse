#include <Parsers/MySQL/ASTDropQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    res->children.clear();
    res->is_truncate = is_truncate;
    res->if_exists = if_exists;
    return res;
}

bool ParserDropQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_truncate(Keyword::TRUNCATE);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword on(Keyword::ON);
    ParserIdentifier name_p(false);

    ParserKeyword s_event(Keyword::EVENT);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_server(Keyword::SERVER);
    ParserKeyword s_trigger(Keyword::TRIGGER);

    auto query = std::make_shared<ASTDropQuery>();
    node = query;
    ASTDropQuery::QualifiedNames names;
    bool if_exists = false;
    bool is_truncate = false;

    if (s_truncate.ignore(pos, expected))
    {
        s_table.ignore(pos, expected);
        is_truncate = true;
        query->kind = ASTDropQuery::Kind::Table;
        ASTDropQuery::QualifiedName name;
        if (parseDatabaseAndTableName(pos, expected, name.schema, name.shortName))
            names.push_back(name);
        else
            return false;
    }
    else if (s_drop.ignore(pos, expected))
    {
        if (s_database.ignore(pos, expected))
        {
            query->kind = ASTDropQuery::Kind::Database;
            if (s_if_exists.ignore(pos, expected))
                if_exists = true;
            ASTPtr database;
            if (!name_p.parse(pos, database, expected))
                return false;
        }
        else
        {
            if (s_view.ignore(pos, expected))
                query->kind = ASTDropQuery::Kind::View;
            else if (s_table.ignore(pos, expected))
                query->kind = ASTDropQuery::Kind::Table;
            else if (s_index.ignore(pos, expected))
            {
                ASTPtr index;
                query->kind = ASTDropQuery::Kind::Index;
                if (!(name_p.parse(pos, index, expected) && on.ignore(pos, expected)))
                    return false;
            }
            else if (s_event.ignore(pos, expected) || s_function.ignore(pos, expected) || s_server.ignore(pos, expected)
                || s_trigger.ignore(pos, expected))
            {
                query->kind = ASTDropQuery::Kind::Other;
            }
            else
                return false;

            if (s_if_exists.ignore(pos, expected))
                if_exists = true;
            //parse name
            auto parse_element = [&]
            {
                ASTDropQuery::QualifiedName element;
                if (parseDatabaseAndTableName(pos, expected, element.schema, element.shortName))
                {
                    names.emplace_back(std::move(element));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;
        }
    }
    else
        return false;

    query->if_exists = if_exists;
    query->names = names;
    query->is_truncate = is_truncate;

    return true;
}

}

}
