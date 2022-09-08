#include "KQLMathematicalFunctions.h"

namespace DB
{
bool IsNan::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "isNaN");
}
}
