#include <Parsers/IParserBase.h>


namespace DB
{

bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Avoid excessive backtracking.
    if (!pos.feasible())
        return false;

    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        bool res = parseImpl(pos, node, expected);
        if (!res)
            node = nullptr;
        return res;
    });
}

}
