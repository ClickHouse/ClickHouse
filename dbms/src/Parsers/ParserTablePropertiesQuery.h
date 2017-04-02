#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос (EXISTS | SHOW CREATE | (DESCRIBE | DESC) ) [TABLE] [db.]name [FORMAT format]
  */
class ParserTablePropertiesQuery : public IParserBase
{
protected:
    const char * getName() const { return "EXISTS, SHOW CREATE or DESCRIBE query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
