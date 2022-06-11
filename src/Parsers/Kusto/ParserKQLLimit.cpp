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
    Int64 min_limit = -1;
    auto final_pos = pos;
    for (auto op_po: op_pos)
    {
        auto is_number = [&]
        {
            for (const auto *ch = op_po->begin ; ch < op_po->end; ++ch)
            {
                if (!isdigit(*ch))
                    return false;
            }
            return true;
        };

        if (!is_number())
            return false;

        auto limit_length = std::strtol(op_po->begin,nullptr, 10);
        if (-1 == min_limit)
        {
            min_limit = limit_length;
            final_pos = op_po;
        }
        else
        {
            if (min_limit > limit_length)
            {
                min_limit = limit_length;
                final_pos = op_po;
            }
        }
    }

    if (!ParserExpressionWithOptionalAlias(false).parse(final_pos, node, expected))
        return false;

    pos = begin;

    return true;
}

}
