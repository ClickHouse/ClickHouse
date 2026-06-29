#pragma once

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

#include <vector>


namespace DB
{

/// After `CREATE SHARD <shard_name>`, parse replica named collections:
/// `REPLICA id1, id2` | `REPLICA (id1, id2)` | `(id1, id2)` (parentheses directly after shard name, with or without whitespace).
inline bool parseSQLClusterShardReplicaCollectionList(std::vector<String> & replicas, IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserIdentifier name_p;
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    auto parse_comma_separated_collections = [&]() -> bool
    {
        ASTPtr replica_id;
        if (!name_p.parse(pos, replica_id, expected))
            return false;
        tryGetIdentifierNameInto(replica_id, replicas.emplace_back());
        while (s_comma.ignore(pos, expected))
        {
            if (!name_p.parse(pos, replica_id, expected))
                return false;
            tryGetIdentifierNameInto(replica_id, replicas.emplace_back());
        }
        return true;
    };

    if (s_replica.ignore(pos, expected))
    {
        if (s_lparen.ignore(pos, expected))
        {
            if (!parse_comma_separated_collections())
                return false;
            return s_rparen.ignore(pos, expected);
        }
        return parse_comma_separated_collections();
    }

    if (s_lparen.ignore(pos, expected))
    {
        if (!parse_comma_separated_collections())
            return false;
        return s_rparen.ignore(pos, expected);
    }

    return false;
}

}
