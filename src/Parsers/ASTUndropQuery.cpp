#include <Parsers/ASTUndropQuery.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Core/UUID.h>


namespace DB
{

String ASTUndropQuery::getID(char delim) const
{
    return "UndropQuery" + (delim + getDatabase()) + delim + getTable();
}

void ASTUndropQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `cluster` is formatted and changes execution from local to distributed DDL, but is not a
    /// child of this query.
    hash_state.update(cluster.size());
    hash_state.update(cluster);
    ASTQueryWithTableAndOutput::updateTreeHashImpl(hash_state, ignore_aliases);
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
