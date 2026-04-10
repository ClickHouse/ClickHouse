#include <Parsers/ASTShowCreateShardQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserShowCreateShardQuery.h>


namespace DB
{

bool ParserShowCreateShardQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show(Keyword::SHOW);
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserIdentifier name_p;

    if (!s_show.ignore(pos, expected))
        return false;
    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_shard.ignore(pos, expected))
        return false;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    auto query = make_intrusive<ASTShowCreateShardQuery>();
    tryGetIdentifierNameInto(name_ast, query->shard_name);
    node = query;
    return true;
}

}
