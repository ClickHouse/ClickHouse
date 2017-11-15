#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Sampling factor of the form 0.1 or 1/10.
  * It is parsed as a rational number without conversion to IEEE-754.
  */
class ParserSampleRatio : public IParserBase
{
protected:
    const char * getName() const { return "Sample ratio or offset"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
