#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <IO/Operators.h>


namespace DB
{
String ASTShowGrantsQuery::getID(char) const
{
    return "ShowGrantsQuery";
}


ASTPtr ASTShowGrantsQuery::clone() const
{
    auto res = std::make_shared<ASTShowGrantsQuery>(*this);

    if (for_roles)
        res->for_roles = std::static_pointer_cast<ASTRolesOrUsersSet>(for_roles->clone());

    return res;
}


void ASTShowGrantsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW GRANTS"
                 ;

    if (for_roles->current_user && !for_roles->all && for_roles->names.empty() && for_roles->except_names.empty()
        && !for_roles->except_current_user)
    {
    }
    else
    {
        ostr << " FOR "
                     ;
        for_roles->format(ostr, settings);
    }

    if (with_implicit)
    {
        ostr << " WITH IMPLICIT"
                     ;
    }

    if (final)
    {
        ostr << " FINAL"
                     ;
    }
}
}
