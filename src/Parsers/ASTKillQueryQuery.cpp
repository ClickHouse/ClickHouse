#include <Parsers/ASTKillQueryQuery.h>
#include <IO/Operators.h>

namespace DB
{

String ASTKillQueryQuery::getID(char delim) const
{
    return String("KillQueryQuery") + delim + (where_expression ? where_expression->getID() : "") + delim + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "KILL ";

    switch (type)
    {
        case Type::Query:
            settings.ostr << "QUERY";
            break;
        case Type::Mutation:
            settings.ostr << "MUTATION";
            break;
        case Type::PartMoveToShard:
            settings.ostr << "PART_MOVE_TO_SHARD";
            break;
        case Type::Transaction:
            settings.ostr << "TRANSACTION";
            break;
    }

    formatOnCluster(settings);

    if (where_expression)
    {
        settings.ostr << " WHERE " << (settings.hilite ? hilite_none : "");
        where_expression->formatImpl(settings, state, frame);
    }

    settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << (test ? "TEST" : (sync ? "SYNC" : "ASYNC")) << (settings.hilite ? hilite_none : "");
}

}
