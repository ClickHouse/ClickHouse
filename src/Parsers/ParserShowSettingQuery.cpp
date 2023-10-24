#include <Parsers/ParserShowSettingQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

bool ParserShowSettingQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword("SHOW SETTING").ignore(pos, expected))
        return false;

    ASTPtr setting_name_identifier;
    if (!ParserIdentifier().parse(pos, setting_name_identifier, expected))
        return false;

    node = std::make_shared<ASTShowSettingQuery>(getIdentifierName(setting_name_identifier));

    return true;
}

}

