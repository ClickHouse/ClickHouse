#include <Parsers/ParserSetConfigQuery.h>

#include <Parsers/ASTSetConfigQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
bool ParserSetConfigQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_set_config("SET CONFIG");
    ParserKeyword s_global("GLOBAL");
    ParserKeyword s_none("NONE");
    ParserToken s_eq(TokenType::Equals);

    ParserCompoundIdentifier name_p;
    ParserLiteral value_p;

    ASTPtr name;
    ASTPtr value;

    auto query = std::make_shared<ASTSetConfigQuery>();

    if (!s_set_config.ignore(pos, expected))
        return false;
    if (s_global.ignore(pos, expected))
        query->is_global = true;
    if (!name_p.parse(pos, name, expected))
        return false;
    if (s_none.ignore(pos, expected))
    {
        query->is_none = true;
    }
    else
    {
        if (s_eq.ignore(pos, expected))
        {
            if (!value_p.parse(pos, value, expected))
                return false;
        }
        else
            return false;
    }

    tryGetIdentifierNameInto(name, query->name);
    if (value)
        query->value = value->as<ASTLiteral &>().value;

    node = query;
    return true;
}

}


