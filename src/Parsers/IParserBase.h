#pragma once

#include <Parsers/IParser.h>


namespace DB
{

/** Base class for most parsers
  */
class IParserBase : public IParser
{
public:
    bool parse(Pos & pos, ASTPtr & node, Expected & expected);

protected:
    virtual bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) = 0;
};

}
