#include <Parsers/ASTKillQueryQuery.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{

String ASTKillQueryQuery::getID(char delim) const
{
    return String("KillQueryQuery") + delim + (where_expression ? where_expression->getID() : "") + delim + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold in the semantic fields kept outside `children` (`where_expression` is hashed through the
    /// child recursion) — see the header comment.
    hash_state.update(type);
    hash_state.update(sync);
    hash_state.update(test);
    hash_state.update(cluster);
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
