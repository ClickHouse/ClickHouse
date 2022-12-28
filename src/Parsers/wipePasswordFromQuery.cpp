#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/wipePasswordFromQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

bool canContainPassword(const IAST & ast)
{
    return ast.as<ASTCreateUserQuery>();
}

void wipePasswordFromQuery(ASTPtr ast)
{
    if (auto * create_query = ast->as<ASTCreateUserQuery>())
    {
        create_query->show_password = false;
    }
}

}
