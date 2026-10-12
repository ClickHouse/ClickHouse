#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
  */
class ASTShowGrantsQuery : public ASTQueryWithOutput
{
public:
    boost::intrusive_ptr<ASTRolesOrUsersSet> for_roles;
    bool with_implicit = false;
    bool final = false;

    String getID(char) const override;

    /// `getID` is a constant, and `for_roles` (whose distinguishing state also lives outside
    /// `children`), `with_implicit` and `final` are plain members. Fold them into the hash so the
    /// rewrite-rule matcher, which treats an equal tree hash as semantic equality, does not
    /// over-match e.g. `SHOW GRANTS` and `SHOW GRANTS FINAL`, or `SHOW GRANTS FOR a` and
    /// `SHOW GRANTS FOR b`.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr clone() const override;
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }
};
}
