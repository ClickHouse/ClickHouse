#pragma once


#include <Parsers/IParserBase.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query (DESCRIBE | DESC) ([TABLE] [db.]name | tableFunction) [FORMAT format]
 */
class ParserDescribeTableQuery : public IParserBase
{
protected:
    const char * getName() const { return "DESCRIBE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
