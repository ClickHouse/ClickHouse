#include <Parsers/ASTShowColumnsQuery.h>

#include <iomanip>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowColumnsQuery::clone() const
{
    auto res = std::make_shared<ASTShowColumnsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowColumnsQuery::formatQueryImpl(FormattingBuffer out) const
{
    out.writeKeyword("SHOW ");
    out.writeKeyword(extended ? "EXTENDED " : "");
    out.writeKeyword(full ? "FULL " : "");
    out.writeKeyword("COLUMNS");
    out.writeKeyword(" FROM ");
    out.ostr << backQuoteIfNeed(from_table);
    if (!from_database.empty())
    {
        out.writeKeyword(" FROM ");
        out.ostr << backQuoteIfNeed(from_database);
    }

    if (!like.empty())
    {
        out.writeKeyword(not_like ? " NOT" : "");
        out.writeKeyword(case_insensitive_like ? " ILIKE " : " LIKE");
        out.ostr << DB::quote << like;
    }

    if (where_expression)
    {
        out.writeKeyword(" WHERE ");
        where_expression->formatImpl(out);
    }

    if (limit_length)
    {
        out.writeKeyword(" LIMIT ");
        limit_length->formatImpl(out);
    }
}

}
