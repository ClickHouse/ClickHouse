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
    settings.writeKeyword("KILL ");

    switch (type)
    {
        case Type::Query:
            settings.writeKeyword("QUERY");
            break;
        case Type::Mutation:
            settings.writeKeyword("MUTATION");
            break;
        case Type::PartMoveToShard:
            settings.writeKeyword("PART_MOVE_TO_SHARD");
            break;
        case Type::Transaction:
            settings.writeKeyword("TRANSACTION");
            break;
    }

    formatOnCluster(settings);

    if (where_expression)
    {
        settings.writeKeyword(" WHERE ");
        where_expression->formatImpl(settings, state, frame);
    }

    settings.ostr << " ";
    settings.writeKeyword(test ? "TEST" : (sync ? "SYNC" : "ASYNC"));
}

}
