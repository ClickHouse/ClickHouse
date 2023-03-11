#include <Parsers/ASTKillQueryQuery.h>
#include <IO/Operators.h>

namespace DB
{

String ASTKillQueryQuery::getID(char delim) const
{
    return String("KillQueryQuery") + delim + (where_expression ? where_expression->getID() : "") + delim + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("KILL ");

    switch (type)
    {
        case Type::Query:
            out.writeKeyword("QUERY");
            break;
        case Type::Mutation:
            out.writeKeyword("MUTATION");
            break;
        case Type::PartMoveToShard:
            out.writeKeyword("PART_MOVE_TO_SHARD");
            break;
        case Type::Transaction:
            out.writeKeyword("TRANSACTION");
            break;
    }

    formatOnCluster(out);

    if (where_expression)
    {
        out.writeKeyword(" WHERE ");
        where_expression->formatImpl(out);
    }

    out.ostr << " ";
    out.writeKeyword(test ? "TEST" : (sync ? "SYNC" : "ASYNC"));
}

}
