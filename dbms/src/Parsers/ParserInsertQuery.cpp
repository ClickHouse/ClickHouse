#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserInsertQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserInsertQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserString s_insert("INSERT", true, true);
    ParserString s_into("INTO", true, true);
    ParserString s_dot(".");
    ParserString s_id("ID");
    ParserString s_eq("=");
    ParserStringLiteral id_p;
    ParserString s_values("VALUES", true, true);
    ParserString s_format("FORMAT", true, true);
    ParserString s_select("SELECT", true, true);
    ParserString s_lparen("(");
    ParserString s_rparen(")");
    ParserIdentifier name_p;
    ParserList columns_p(std::make_unique<ParserCompoundIdentifier>(), std::make_unique<ParserString>(","), false);

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;
    ASTPtr format;
    ASTPtr select;
    ASTPtr id;
    /// Insertion data
    const char * data = nullptr;

    ws.ignore(pos, end);

    /// INSERT INTO
    if (!s_insert.ignore(pos, end, max_parsed_pos, expected)
        || !ws.ignore(pos, end)
        || !s_into.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (s_dot.ignore(pos, end, max_parsed_pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    ws.ignore(pos, end);

    if (s_id.ignore(pos, end, max_parsed_pos, expected))
    {
        if (!s_eq.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!id_p.parse(pos, end, id, max_parsed_pos, expected))
            return false;
    }

    ws.ignore(pos, end);

    /// Is there a list of columns
    if (s_lparen.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!columns_p.parse(pos, end, columns, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!s_rparen.ignore(pos, end, max_parsed_pos, expected))
            return false;
    }

    ws.ignore(pos, end);

    Pos before_select = pos;

    /// VALUES or FORMAT or SELECT
    if (s_values.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);
        data = pos;
        pos = end;
    }
    else if (s_format.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!name_p.parse(pos, end, format, max_parsed_pos, expected))
            return false;

        /// Data starts after the first newline, if there is one, or after all the whitespace characters, otherwise.
        ParserWhiteSpaceOrComments ws_without_nl(false);

        ws_without_nl.ignore(pos, end);
        if (pos != end && *pos == ';')
            throw Exception("You have excessive ';' symbol before data for INSERT.\n"
                "Example:\n\n"
                "INSERT INTO t (x, y) FORMAT TabSeparated\n"
                "1\tHello\n"
                "2\tWorld\n"
                "\n"
                "Note that there is no ';' in first line.", ErrorCodes::SYNTAX_ERROR);

        if (pos != end && *pos == '\n')
            ++pos;

        data = pos;
        pos = end;
    }
    else if (s_select.ignore(pos, end, max_parsed_pos, expected))
    {
        pos = before_select;
        ParserSelectQuery select_p;
        select_p.parse(pos, end, select, max_parsed_pos, expected);
    }
    else
    {
        expected = "VALUES or FORMAT or SELECT";
        return false;
    }

    auto query = std::make_shared<ASTInsertQuery>(StringRange(begin, data ? data : pos));
    node = query;

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;

    query->table = typeid_cast<ASTIdentifier &>(*table).name;

    if (id)
        query->insert_id = safeGet<const String &>(typeid_cast<ASTLiteral &>(*id).value);

    if (format)
        query->format = typeid_cast<ASTIdentifier &>(*format).name;

    query->columns = columns;
    query->select = select;
    query->data = data != end ? data : NULL;
    query->end = end;

    if (columns)
        query->children.push_back(columns);
    if (select)
        query->children.push_back(select);

    return true;
}


}
