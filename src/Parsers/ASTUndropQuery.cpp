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
