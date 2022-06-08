#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLLimit.h>
#include <cstdlib>

namespace DB
{

bool ParserKQLLimit :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;

    auto begin = pos;
    Int64 minLimit = -1;
    auto final_pos = pos;
    for (auto it = op_pos.begin(); it != op_pos.end(); ++it)
    {
        pos = *it;
        auto isNumber = [&]
        {
            for (auto ch = pos->begin ; ch < pos->end; ++ch)
            {
                if (!isdigit(*ch))
                    return false;
            }
            return true;
        };

        if (!isNumber())
            return false;

        auto limitLength = std::strtol(pos->begin,nullptr, 10);
        if (-1 == minLimit)
        {
            minLimit = limitLength;
            final_pos = pos;
        }
        else
        {
            if (minLimit > limitLength)
            {
                minLimit = limitLength;
                final_pos = pos;
            }
        }
    }

    if (!ParserExpressionWithOptionalAlias(false).parse(final_pos, node, expected))
        return false;

    pos = begin;

    return true;
}

}
