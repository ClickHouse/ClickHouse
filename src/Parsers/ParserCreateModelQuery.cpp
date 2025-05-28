#include <Parsers/ParserCreateModelQuery.h>
#include <Parsers/ASTCreateModelQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserCreateModelQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto model = std::make_shared<ASTCreateModelQuery>();

    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_model(Keyword::MODEL);
    ParserKeyword s_algorithm(Keyword::ALGORITHM);
    ParserKeyword s_options(Keyword::OPTIONS);
    ParserKeyword s_target(Keyword::TARGET);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_table(Keyword::TABLE);

    ParserCompoundIdentifier model_name_p;
    ParserStringLiteral algorithm_p;
    ParserSetQuery options_p(true);
    ParserStringLiteral target_p;
    ParserCompoundIdentifier table_name_p;

    // CREATE MODEL

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_model.ignore(pos, expected))
        return false;

    ASTPtr model_name;
    if (!model_name_p.parse(pos, model_name, expected))
        return false;

    // ALGORITHM

    if (!s_algorithm.ignore(pos, expected))
        return false;

    ASTPtr algorithm;
    if (!algorithm_p.parse(pos, algorithm, expected))
        return false;

    // OPTIONS (optional field)

    ASTPtr options;
    if (s_options.ignore(pos, expected))
    {
        if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
            return false;

        if (!options_p.parse(pos, options, expected))
            return false;

        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;
    }

    // TARGET

    if (!s_target.ignore(pos, expected))
        return false;

    ASTPtr target;
    if (!target_p.parse(pos, target, expected))
        return false;

    // FROM TABLE

    if (!s_from.ignore(pos, expected))
        return false;

    // TODO: Add tableFunctions here

    if (!s_table.ignore(pos, expected))
        return false;

    ASTPtr table_name;
    if (!table_name_p.parse(pos, table_name, expected))
        return false;

    // OK

    model->model_name = model_name;
    model->algorithm = algorithm;
    model->options = options;
    model->target = target;
    model->table_name = table_name;

    node = model;
    return true;
}

}
