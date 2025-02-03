#include <IO/Operators.h>
#include <Parsers/ASTDeduceQuery.h>
#include <Common/quoteString.h>

namespace DB
{

void ASTDeduceQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "DEDUCE TABLE " << (settings.hilite ? hilite_none : "");

    chassert(table);
    table->format(ostr, settings, state, frame);
    ostr << " BY ";
    col_to_deduce->format(ostr, settings);
}

}
