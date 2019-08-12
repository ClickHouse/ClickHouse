#pragma once


#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/** Query (DESCRIBE | DESC) ([TABLE] [db.]name | tableFunction) [FORMAT format]
 */
class ParserDescribeTableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DESCRIBE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
