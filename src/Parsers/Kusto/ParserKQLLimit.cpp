#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLLimit.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <cstdlib>
#include <format>

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

    String sub_query = std::format("( SELECT * FROM {} LIMIT {} )", table_name, String(final_pos->begin, final_pos->end));

    Tokens token_subquery(sub_query.c_str(), sub_query.c_str() + sub_query.size());
    IParser::Pos pos_subquery(token_subquery, pos.max_depth);

    if (!ParserTablesInSelectQuery().parse(pos_subquery, node, expected))
        return false;

    pos = begin;

    return true;
}

}
