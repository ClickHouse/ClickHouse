#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTLiteral.h>

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

void ASTShowColumnsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << (full ? "FULL " : "")
                  << "COLUMNS"
                 ;

    ostr << " FROM " << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << " FROM " << backQuoteIfNeed(database);


    if (!like.empty())
    {
        ostr

            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE")
            << quoteString(like);
    }

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }

    if (limit_length)
    {
        ostr << " LIMIT ";
        limit_length->format(ostr, settings, state, frame);
    }
}

}
