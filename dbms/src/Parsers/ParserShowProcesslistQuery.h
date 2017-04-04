#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTShowProcesslistQuery.h>


namespace DB
{

/** Запрос SHOW PROCESSLIST
  */
class ParserShowProcesslistQuery : public IParserBase
{
protected:
    const char * getName() const { return "SHOW PROCESSLIST query"; }

    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
    {
        Pos begin = pos;

        ParserWhiteSpaceOrComments ws;
        ParserString s_show("SHOW", true, true);
        ParserString s_processlist("PROCESSLIST", true, true);

        auto query = std::make_shared<ASTShowProcesslistQuery>();

        ws.ignore(pos, end);

        if (!s_show.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!s_processlist.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        query->range = StringRange(begin, pos);
        node = query;

        return true;
    }
};

}
