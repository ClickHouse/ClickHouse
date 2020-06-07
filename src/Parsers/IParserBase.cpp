#include <Parsers/IParserBase.h>


namespace DB
{

bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        Pos prev_pos = pos;
        size_t prev_ranges_size = 0;
        if (ranges)
            prev_ranges_size = ranges->size();

        bool res = parseImpl(pos, node, expected, ranges);

        if (res)
        {
            if (ranges)
            {
                Range range;
                range.begin = prev_pos;
                range.end = pos;
                range.color = color();
                if (range.color)
                    ranges->emplace_back(std::move(range));
            }
        }
        else
        {
            node = nullptr;
        }

        return res;
    });
}

}
