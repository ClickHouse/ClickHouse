#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseUserName.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseRoleName(pos, expected, new_name);
        });
    }
}


bool ParserCreateRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool attach = false;
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH ROLE"}.ignore(pos, expected))
            return false;
        attach = true;
    }
    else
    {
        if (ParserKeyword{"ALTER ROLE"}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{"CREATE ROLE"}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected))
            or_replace = true;
    }

    String name;
    if (!parseRoleName(pos, expected, name))
        return false;

    String new_name;
    if (alter)
        parseRenameTo(pos, expected, new_name);

    auto query = std::make_shared<ASTCreateRoleQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->name = std::move(name);
    query->new_name = std::move(new_name);

    return true;
}
}
