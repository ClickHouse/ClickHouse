#pragma once

#include <Parsers/IAST.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
  * SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
  */
class ASTSetRoleQuery : public IAST
{
public:
    enum class Kind : uint8_t
    {
        SET_ROLE,
        SET_ROLE_DEFAULT,
        SET_DEFAULT_ROLE,
    };
    Kind kind = Kind::SET_ROLE;

    boost::intrusive_ptr<ASTRolesOrUsersSet> roles;
    boost::intrusive_ptr<ASTRolesOrUsersSet> to_users;

    String getID(char) const override;
    ASTPtr clone() const override;

    /// `getID` returns a constant string, and `kind` / `roles` / `to_users` are plain members,
    /// not part of `children` (this AST has none). Without folding them into the hash,
    /// `SET ROLE a` and `SET ROLE b` (or `SET DEFAULT ROLE r TO u1` and `... TO u2`) would share
    /// one tree hash. The rewrite-rule matcher treats an equal tree hash as semantic equality,
    /// so a rule template for one `SET ROLE` would over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    QueryKind getQueryKind() const override { return QueryKind::Set; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
