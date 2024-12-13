#include "Parsers/ZetaSQL/ParserAggregateStatement.h"
#include <memory>
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/CommonParsers.h"
#include "Parsers/ExpressionListParsers.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/Lexer.h"
#include "Parsers/ZetaSQL/ASTZetaSQLQuery.h"

namespace DB::ZetaSQL {
    bool ParserAggregateStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {

        ParserList function_list(
            std::make_unique<ParserExpressionWithOptionalAlias>(true),
            std::make_unique<ParserToken>(TokenType::Comma),
            false
        );

        ASTPtr aggregate_expressions;
        if (!function_list.parse(pos, aggregate_expressions, expected))
            return false;

        for(const auto & expr : aggregate_expressions->children)
        {
            if(!expr->as<ASTFunction>())
                return false;
        }

        ASTPtr group_by_expr;
        if(ParserKeyword(Keyword::GROUP_BY).ignore(pos, expected))
        {
            // no aliases allowed in group by
            ParserExpressionList group_by_parser(false);
            if(!group_by_parser.parse(pos, group_by_expr, expected))
                return false;
        }

        //Collect the expressions under a SELECT query
    auto select_expr = std::make_shared<ASTSelectQuery>();
        select_expr->setExpression(ASTSelectQuery::Expression::SELECT, std::move(aggregate_expressions));
        if(group_by_expr)
            select_expr->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_expr));

        auto * zetasql_ast = node->as<ASTZetaSQLQuery>();

        zetasql_ast->stages.push_back({
            ASTZetaSQLQuery::StageKeyword::AGGREGATE,
            select_expr
        });

        return true;
    }
}
