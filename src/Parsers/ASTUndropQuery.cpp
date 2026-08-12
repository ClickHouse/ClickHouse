#include <Parsers/ASTUndropQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Core/UUID.h>


namespace DB
{

String ASTUndropQuery::getID(char delim) const
{
    return "UndropQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTUndropQuery::clone() const
{
    auto res = make_intrusive<ASTUndropQuery>(*this);
    /// The copy constructor shares `children` with the source; rebuild them as deep copies in the
    /// parser's order - the table first, the output options last - so the clone is independent of
    /// the source and has the same tree hash.
    res->children.clear();
    cloneTableOptions(*res);
    cloneOutputOptions(*res);
    return res;
}

void ASTUndropQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
        << "UNDROP TABLE"

        << " ";

    chassert(table);

    if (table)
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);
    }

    if (uuid != UUIDHelpers::Nil)
        ostr << " UUID "
            << quoteString(toString(uuid));

    formatOnCluster(ostr, settings);
}

}
