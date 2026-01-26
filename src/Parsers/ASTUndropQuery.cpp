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
    auto res = make_intrusive<ASTUndropQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTUndropQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
        << "UNDROP TABLE"

        << " ";

    auto tbl = getTableAst();
    chassert(tbl);

    if (tbl)
    {
        if (auto db = getDatabaseAst())
        {
            db->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(tbl);
        tbl->format(ostr, settings, state, frame);
    }

    if (uuid != UUIDHelpers::Nil)
        ostr << " UUID "
            << quoteString(toString(uuid));

    formatOnCluster(ostr, settings);
}

}
