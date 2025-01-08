#include <Parsers/ZetaSQL/ParserFromStatement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ZetaSQL/ASTZetaSQLQuery.h>

namespace DB::ZetaSQL
{
    bool ParserFromStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ParserTablesInSelectQuery tables_parser;
        ASTPtr tables;

        if(!ParserKeyword(Keyword::FROM).ignore(pos, expected))
            return false;

        auto * query_ast = node->as<ASTZetaSQLQuery>();

        if(query_ast->stages.size() != 0) {
            return false;
        }

        if(!tables_parser.parse(pos, tables, expected))
            return false;

        query_ast->stages.push_back({
            ASTZetaSQLQuery::StageKeyword::FROM,
            tables
        });

        return true;
    }
}
