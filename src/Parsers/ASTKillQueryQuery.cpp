#include <Parsers/ASTKillQueryQuery.h>
#include <IO/Operators.h>

namespace DB
{

String ASTKillQueryQuery::getID(char delim) const
{
    return String("KillQueryQuery") + delim + (where_expression ? where_expression->getID() : "") + delim + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "KILL ";

    switch (type)
    {
        case Type::Query:
            ostr << "QUERY";
            break;
        case Type::Mutation:
            ostr << "MUTATION";
            break;
        case Type::PartMoveToShard:
            ostr << "PART_MOVE_TO_SHARD";
            break;
        case Type::Transaction:
            ostr << "TRANSACTION";
            break;
    }

    formatOnCluster(ostr, settings);

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }

    ostr << " " << (test ? "TEST" : (sync ? "SYNC" : "ASYNC"));
}

}
