#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{
String ASTShowGrantsQuery::getID(char) const
{
    return "ShowGrantsQuery";
}


void ASTShowGrantsQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives the
    /// format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(for_roles != nullptr);
    if (for_roles)
        for_roles->updateTreeHash(hash_state, ignore_aliases);
    hash_state.update(with_implicit);
    hash_state.update(final);
}


ASTPtr ASTShowGrantsQuery::clone() const
{
    auto res = make_intrusive<ASTShowGrantsQuery>(*this);

    if (for_roles)
        res->for_roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(for_roles->clone());

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
