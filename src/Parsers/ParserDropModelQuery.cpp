#include <Parsers/ParserDropModelQuery.h>
#include <Parsers/ASTDropModelQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserDropModelQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto drop_model_query = std::make_shared<ASTDropModelQuery>();

    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_model(Keyword::MODEL);
    ParserCompoundIdentifier model_name_p;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_model.ignore(pos, expected))
        return false;

    ASTPtr model_name;
    if (!model_name_p.parse(pos, model_name, expected))
        return false;

    drop_model_query->model_name = model_name;

    node = drop_model_query;
    return true;
}

}
