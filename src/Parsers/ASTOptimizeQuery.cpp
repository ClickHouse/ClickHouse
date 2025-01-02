#include <Parsers/ASTOptimizeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

void ASTOptimizeQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (settings.hilite ? hilite_none : "");

    if (database)
    {
        database->formatImpl(ostr, settings, state, frame);
        ostr << '.';
    }

    chassert(table);
    table->formatImpl(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    if (partition)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(ostr, settings, state, frame);
    }

    if (final)
        ostr << (settings.hilite ? hilite_keyword : "") << " FINAL" << (settings.hilite ? hilite_none : "");

    if (deduplicate)
        ostr << (settings.hilite ? hilite_keyword : "") << " DEDUPLICATE" << (settings.hilite ? hilite_none : "");

    if (cleanup)
        ostr << (settings.hilite ? hilite_keyword : "") << " CLEANUP" << (settings.hilite ? hilite_none : "");

    if (deduplicate_by_columns)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " BY " << (settings.hilite ? hilite_none : "");
        deduplicate_by_columns->formatImpl(ostr, settings, state, frame);
    }
}

}
