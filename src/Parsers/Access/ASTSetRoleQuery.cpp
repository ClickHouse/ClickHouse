#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
String ASTSetRoleQuery::getID(char) const
{
    return "SetRoleQuery";
}


ASTPtr ASTSetRoleQuery::clone() const
{
    auto res = std::make_shared<ASTSetRoleQuery>(*this);

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (to_users)
        res->to_users = std::static_pointer_cast<ASTRolesOrUsersSet>(to_users->clone());

    return res;
}


void ASTSetRoleQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    switch (kind)
    {
        case Kind::SET_ROLE: settings.writeKeyword("SET ROLE"); break;
        case Kind::SET_ROLE_DEFAULT: settings.writeKeyword("SET ROLE DEFAULT"); break;
        case Kind::SET_DEFAULT_ROLE: settings.writeKeyword("SET DEFAULT ROLE"); break;
    }

    if (kind == Kind::SET_ROLE_DEFAULT)
        return;

    settings.ostr << " ";
    roles->format(settings);

    if (kind == Kind::SET_ROLE)
        return;

    settings.writeKeyword(" TO ");
    to_users->format(settings);
}
}
