#include <Parsers/IParserBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;
    expected.add(pos, getName());

    pos.increaseDepth();
    bool res = parseImpl(pos, node, expected);
    pos.decreaseDepth();

    if (!res)
    {
        node = nullptr;
        pos = begin;
    }

    return res;
}

}
