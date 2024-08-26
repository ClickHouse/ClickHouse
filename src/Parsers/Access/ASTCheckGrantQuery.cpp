#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include "Common/logger_useful.h"
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

String ASTCheckGrantQuery::getID(char) const
{
    return "CheckGrantQuery";
}


ASTPtr ASTCheckGrantQuery::clone() const
{
    auto res = std::make_shared<ASTCheckGrantQuery>(*this);

    return res;
}


void ASTCheckGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}

}
