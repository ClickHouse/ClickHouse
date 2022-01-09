#include <Parsers/IParserBase.h>


namespace DB
{

bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos.failure_no_backtrack)
        return false;

    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        bool res = parseImpl(pos, node, expected);
        if (!res)
        {
            node = nullptr;
            if (pos.no_backtrack_if_failure)
                pos.failure_no_backtrack = true;
        }
        pos.no_backtrack_if_failure = false;
        return res;
    });
}

}
