#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/SipHash.h>
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
    auto res = make_intrusive<ASTSetRoleQuery>(*this);

    if (roles)
        res->roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (to_users)
        res->to_users = boost::static_pointer_cast<ASTRolesOrUsersSet>(to_users->clone());

    return res;
}


void ASTSetRoleQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Mirror the formatter exactly — it emits `roles` only for
    /// `SET ROLE` / `SET DEFAULT ROLE` and `to_users` only for `SET DEFAULT ROLE` — so every
    /// folded field survives the format -> parse round-trip that the debug-build AST consistency
    /// check requires.
    hash_state.update(kind);

    if (kind == Kind::SET_ROLE_DEFAULT)
        return;

    hash_state.update(static_cast<bool>(roles));
    if (roles)
        roles->updateTreeHash(hash_state, ignore_aliases);

    if (kind == Kind::SET_ROLE)
        return;

    hash_state.update(static_cast<bool>(to_users));
    if (to_users)
        to_users->updateTreeHash(hash_state, ignore_aliases);
}


void ASTSetRoleQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    switch (kind)
    {
        case Kind::SET_ROLE: ostr << "SET ROLE"; break;
        case Kind::SET_ROLE_DEFAULT: ostr << "SET ROLE DEFAULT"; break;
        case Kind::SET_DEFAULT_ROLE: ostr << "SET DEFAULT ROLE"; break;
    }

    if (kind == Kind::SET_ROLE_DEFAULT)
        return;

    ostr << " ";
    roles->format(ostr, settings);

    if (kind == Kind::SET_ROLE)
        return;

    ostr << " TO ";
    to_users->format(ostr, settings);
}
}
