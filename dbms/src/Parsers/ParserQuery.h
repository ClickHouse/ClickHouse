#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserQuery : public IParserBase
{
private:
    const char * end;
protected:
    ParserInsertQuery(const char * end) : end(end) {}
    const char * getName() const { return "Query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
