#include <Parsers/Access/ParserExecuteAsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>


namespace DB
{
namespace
{
    bool parseTargetUser(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTRolesOrUsersSet> & targetuser)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserRolesOrUsersSet user_p;
            user_p.allowUsers();
            if (!user_p.parse(pos, ast, expected))
                return false;

            targetuser = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            targetuser->allow_roles = false;
            return true;
        });
    }
}


bool ParserExecuteAsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::shared_ptr<ASTRolesOrUsersSet> targetuser;

    if (ParserKeyword{Keyword::EXECUTE_AS}.ignore(pos, expected))
    {
        if (!parseTargetUser(pos, expected, targetuser))
            return false;
    }
    else
        return false;

    auto query = std::make_shared<ASTExecuteAsQuery>();
    node = query;

    query->targetuser = targetuser;

    /// support 1) EXECUTE AS <user1>  2) EXECUTE AS <user1> SELECT ...

    if (ParserKeyword{Keyword::SELECT}.checkWithoutMoving(pos, expected))
    {
        ParserSelectWithUnionQuery select_p;
        ASTPtr select;

        if (!select_p.parse(pos, select, expected))
            return false;
        query->set(query->select, select);
    }
    return true;
}

}
