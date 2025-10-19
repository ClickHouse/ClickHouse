#include <Parsers/IParserBase.h>
#include <Parsers/IAST.h>


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
            if (pos->begin > begin)
            {
                Pos prev_token = pos;
                --prev_token;

                auto query_begin = pos.getQueryBegin();
                node->setLocation(begin - query_begin, prev_token->end - query_begin);

                Highlight type = highlight();
                if (type != Highlight::none)
                {
                    HighlightedRange range;
                    range.begin = begin;
                    range.end = prev_token->end;
                    range.highlight = type;

                    expected.highlight(range);
                }
            }
        }
        else
            node = nullptr;
        return res;
    });
}

}
