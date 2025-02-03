#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
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


void ASTShowGrantsQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW GRANTS"
                  << (settings.hilite ? hilite_none : "");

    if (for_roles->current_user && !for_roles->all && for_roles->names.empty() && for_roles->except_names.empty()
        && !for_roles->except_current_user)
    {
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FOR "
                      << (settings.hilite ? hilite_none : "");
        for_roles->format(settings);
    }

    if (with_implicit)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH IMPLICIT"
                      << (settings.hilite ? hilite_none : "");
    }

    if (final)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FINAL"
                      << (settings.hilite ? hilite_none : "");
    }
}
}
