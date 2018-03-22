#include <Parsers/ASTKillQueryQuery.h>

namespace DB
{

String ASTKillQueryQuery::getID() const
{
    return "KillQueryQuery_" + (where_expression ? where_expression->getID() : "") + "_" + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "KILL QUERY WHERE " << (settings.hilite ? hilite_none : "");

    if (where_expression)
        where_expression->formatImpl(settings, state, frame);

    settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << (test ? "TEST" : (sync ? "SYNC" : "ASYNC")) << (settings.hilite ? hilite_none : "");
}

}
