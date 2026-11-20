#pragma once


#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query (DESCRIBE | DESC) ([TABLE] [db.]name | tableFunction) [FORMAT format]
 */
class ParserDescribeTableQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "DESCRIBE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
