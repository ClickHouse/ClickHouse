#include <Parsers/ParserDescribeCacheQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{


bool ParserDescribeCacheQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword p_describe(Keyword::DESCRIBE);
    ParserKeyword p_desc(Keyword::DESC);
    ParserKeyword p_cache(Keyword::FILESYSTEM_CACHE);
    ParserLiteral p_cache_name;

    if ((!p_describe.ignore(pos, expected) && !p_desc.ignore(pos, expected))
        || !p_cache.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTDescribeCacheQuery>();

    ASTPtr ast;
    if (!p_cache_name.parse(pos, ast, expected))
        return false;

    query->cache_name = ast->as<ASTLiteral>()->value.safeGet<String>();
    node = query;

    return true;
}


}
