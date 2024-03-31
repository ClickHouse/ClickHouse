#include <Parsers/IParserBase.h>


namespace DB
{

bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        const char * begin = pos->begin;
        bool res = parseImpl(pos, node, expected);
        if (res)
        {
            Highlight type = highlight();
            if (type != Highlight::none)
            {
                HighlightedRange range;
                range.begin = begin;
                range.end = pos->begin;
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
