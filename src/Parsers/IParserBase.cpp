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
        const char * begin = pos->begin;
        bool res = parseImpl(pos, node, expected);
        if (res)
        {
            Highlight type = highlight();
            if (pos->begin > begin && type != Highlight::none)
            {
                Pos prev_token = pos;
                --prev_token;

                HighlightedRange range;
                range.begin = begin;
                range.end = prev_token->end;
                range.highlight = type;

                expected.highlight(range);
            }
        }
        else
            node = nullptr;
        return res;
    });
}

}
