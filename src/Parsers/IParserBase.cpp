#include <Parsers/IParserBase.h>
#include <iostream>


namespace DB
{

bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Limit backtracking.
    if (expected.max_greedy_pos && pos->begin < expected.max_greedy_pos)
        return false;

    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        /// If you want to debug the parser, write this: std::cerr << std::string(pos.depth, ' ') << getName() << ": " << pos.get().begin << "\n";
        bool res = parseImpl(pos, node, expected);
        if (!res)
            node = nullptr;
        return res;
    });
}

}
