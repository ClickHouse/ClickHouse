#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Коэффициент сэмплирования вида 0.1 или 1/10.
  * Парсится как рациональное число без преобразования в IEEE-754.
  */
class ParserSampleRatio : public IParserBase
{
protected:
    const char * getName() const { return "Sample ratio or offset"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
