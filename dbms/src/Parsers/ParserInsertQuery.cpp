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


bool ParserInsertQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    ParserKeyword s_insert_into("INSERT INTO");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_values("VALUES");
    ParserKeyword s_format("FORMAT");
    ParserKeyword s_select("SELECT");
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserList columns_p(std::make_unique<ParserCompoundIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;
    ASTPtr format;
    ASTPtr select;
    /// Insertion data
    const char * data = nullptr;

    if (!s_insert_into.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    /// Is there a list of columns
    if (s_lparen.ignore(pos, expected))
    {
        if (!columns_p.parse(pos, columns, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    Pos before_select = pos;

    /// VALUES or FORMAT or SELECT
    if (s_values.ignore(pos, expected))
    {
        data = pos->begin;
    }
    else if (s_format.ignore(pos, expected))
    {
        auto name_pos = pos;

        if (!name_p.parse(pos, format, expected))
            return false;

        data = name_pos->end;

        if (data < end && *data == ';')
            throw Exception("You have excessive ';' symbol before data for INSERT.\n"
                                    "Example:\n\n"
                                    "INSERT INTO t (x, y) FORMAT TabSeparated\n"
                                    ";\tHello\n"
                                    "2\tWorld\n"
                                    "\n"
                                    "Note that there is no ';' just after format name, "
                                    "you need to put at least one whitespace symbol before the data.", ErrorCodes::SYNTAX_ERROR);

        while (data < end && (*data == ' ' || *data == '\t' || *data == '\f'))
            ++data;

        /// Data starts after the first newline, if there is one, or after all the whitespace characters, otherwise.

        if (data < end && *data == '\r')
            ++data;

        if (data < end && *data == '\n')
            ++data;
    }
    else if (s_select.ignore(pos, expected))
    {
        pos = before_select;
        ParserSelectQuery select_p;
        select_p.parse(pos, select, expected);
    }
    else
    {
        return false;
    }

    auto query = std::make_shared<ASTInsertQuery>(StringRange(begin, pos));
    node = query;

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;

    query->table = typeid_cast<ASTIdentifier &>(*table).name;

    if (format)
        query->format = typeid_cast<ASTIdentifier &>(*format).name;

    query->columns = columns;
    query->select = select;
    query->data = data != end ? data : nullptr;
    query->end = end;

    if (columns)
        query->children.push_back(columns);
    if (select)
        query->children.push_back(select);

    return true;
}


}
