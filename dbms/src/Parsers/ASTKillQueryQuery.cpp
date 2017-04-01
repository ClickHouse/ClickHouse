#include <Parsers/ASTKillQueryQuery.h>

namespace DB
{

String ASTKillQueryQuery::getID() const
{
    return "KillQueryQuery_" + (where_expression ? where_expression->getID() : "") + "_" + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << "KILL QUERY WHERE ";

    if (where_expression)
        where_expression->formatImpl(settings, state, frame);

    settings.ostr << " " << (test ? "TEST" : (sync ? "SYNC" : "ASYNC"));
}

}
