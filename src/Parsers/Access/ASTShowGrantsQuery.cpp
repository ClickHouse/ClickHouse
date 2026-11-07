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
    auto res = make_intrusive<ASTShowGrantsQuery>(*this);

    if (for_roles)
        res->for_roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(for_roles->clone());

    /// Re-add the children in the order the parser produces them: the query first, then the output options.
    res->children.clear();
    res->for_query = nullptr;
    if (for_query)
        res->set(res->for_query, for_query->clone());
    cloneOutputOptions(*res);

    return res;
}


void ASTShowGrantsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "SHOW GRANTS"
                 ;

    if (for_roles->current_user && !for_roles->all && (!for_roles->names || for_roles->names->children.empty())
        && (!for_roles->except_names || for_roles->except_names->children.empty()) && !for_roles->except_current_user)
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

    if (for_query)
    {
        ostr << " FOR QUERY" << settings.nl_or_ws;
        for_query->format(ostr, settings, state, frame);
    }
}
}
