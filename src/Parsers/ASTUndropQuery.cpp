#include <Parsers/ASTUndropQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

String ASTUndropQuery::getID(char delim) const
{
    return "UndropQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTUndropQuery::clone() const
{
    auto res = std::make_shared<ASTUndropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTUndropQuery::formatQueryImpl(IAST::FormattingBuffer out) const
{
    out.writeKeyword("UNDROP TABLE ");

    assert (table);
    if (!database)
        out.ostr << backQuoteIfNeed(getTable());
    else
        out.ostr << backQuoteIfNeed(getDatabase()) + "." << backQuoteIfNeed(getTable());

    if (uuid != UUIDHelpers::Nil)
    {
        out.writeKeyword(" UUID ");
        out.ostr << quoteString(toString(uuid));
    }

    formatOnCluster(out);
}

}
