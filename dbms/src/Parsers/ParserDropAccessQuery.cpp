#include <Parsers/ParserDropAccessQuery.h>
#include <Parsers/ASTDropAccessQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateAccessQuery.h>


namespace DB
{
bool ParserDropAccessQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword drop_p("DROP");
    if (!drop_p.ignore(pos, expected))
        return false;

    using Kind = ASTDropAccessQuery::Kind;
    Kind kind;
    if (ParserKeyword{"ROLE"}.ignore(pos, expected))
        kind = Kind::ROLE;
    else if (ParserKeyword{"USER"}.ignore(pos, expected))
        kind = Kind::USER;
    else
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    if ((kind == Kind::ROLE) || (kind == Kind::USER))
    {
        ParserToken comma{TokenType::Comma};
        do
        {
            String name;
            if (!parseRoleNameAtHost(pos, name, expected))
                return false;
            names.emplace_back(std::move(name));
        }
        while (comma.ignore(pos, expected));
    }

    auto query = std::make_shared<ASTDropAccessQuery>();
    node = query;

    query->names = std::move(names);
    query->if_exists = if_exists;
    query->kind = kind;

    return true;
}
}
