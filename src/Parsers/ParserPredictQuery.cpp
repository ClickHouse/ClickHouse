#include <Parsers/ParserPredictQuery.h>
#include <Parsers/ASTPredictQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserPredictQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto model = std::make_shared<ASTPredictQuery>();

    ParserKeyword s_predict(Keyword::PREDICT);
    ParserKeyword s_model(Keyword::MODEL);
    ParserKeyword s_table(Keyword::TABLE);

    ParserCompoundIdentifier model_p;
    ParserCompoundIdentifier table_p;

    if (!s_predict.ignore(pos, expected))
        return false;

    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        return false;

    if (!s_model.ignore(pos, expected))
        return false;

    ASTPtr model_name;
    if (!model_p.parse(pos, model_name, expected))
        return false;

    if (!s_table.ignore(pos, expected))
        return false;

    ASTPtr table_name;
    if (!table_p.parse(pos, table_name, expected))
        return false;

    if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
        return false;

    model->model_name = model_name;
    model->table_name = table_name;

    node = model;
    return true;
}

}
