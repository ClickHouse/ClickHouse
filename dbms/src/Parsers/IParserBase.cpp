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

    bool res = parseImpl(pos, node, expected);

    if (!res)
    {
        node = nullptr;
        pos = begin;
    }
    else if (node)
        node->range = StringRange(begin, pos);

    return res;
}

}
