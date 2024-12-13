#include "Parsers/ZetaSQL/ParserWhereClause.h"
#include "Parsers/CommonParsers.h"
#include "Parsers/ExpressionListParsers.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/IParser.h"
#include "Parsers/ZetaSQL/ASTZetaSQLQuery.h"

namespace DB::ZetaSQL
{
    bool ParserWhereClause::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ParserExpression expression_parser;
        ASTPtr where_expression;

        if(!ParserKeyword(Keyword::WHERE).ignore(pos, expected))
            return false;

        ASTPtr where_expr;
        if(!expression_parser.parse(pos, where_expr, expected))
            return false;

        auto * query_ast = node->as<ASTZetaSQLQuery>();
        query_ast->stages.push_back({
            ASTZetaSQLQuery::StageKeyword::WHERE,
            where_expr
        });

        return true;
    }
}
