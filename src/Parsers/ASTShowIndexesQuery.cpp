#include <Parsers/ASTShowIndexesQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowIndexesQuery::clone() const
{
    auto res = make_intrusive<ASTShowIndexesQuery>(*this);
    res->children.clear();
    /// `where_expression` is not a child: the parser puts it into the member only. Do not leave it
    /// shared with the source.
    if (where_expression)
        res->where_expression = where_expression->clone();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowIndexesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << "INDEXES"
                 ;

    ostr << " FROM " << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << " FROM " << backQuoteIfNeed(database);

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }
}

}
