#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/Common/AccessRightsElement.h>


namespace DB
{

/** CREATE TOKEN
  *     [{VALID UNTIL datetime | VALID FOR interval}]
  *     [GRANTS (privilege ON object [,...])]
  *
  * Syntactic sugar for adding an additional authentication method to the current user:
  * the server generates a random secret, stores it as a password of the current user and
  * returns it in the result of the query.
  */
class ASTCreateTokenQuery : public ASTQueryWithOutput
{
public:
    /// The deadline from `VALID UNTIL <datetime>` or `VALID FOR <interval>`. It is registered in
    /// `children` (before the output options, which the parser appends afterwards), so that the
    /// generic AST machinery - depth/size limits, clone-based visitors - sees the subtree; assign
    /// it only through `setValidUntil` to keep the two in sync.
    ASTPtr valid_until;
    /// If true, `valid_until` holds an interval expression coming from `VALID FOR <interval>`
    /// (the deadline is `now` plus the interval); otherwise it holds a `VALID UNTIL` value.
    bool valid_until_is_interval = false;

    void setValidUntil(ASTPtr ast);

    /// If not empty, the access rights of a session authenticated with the token are limited to
    /// the intersection with these elements (the GRANTS clause).
    AccessRightsElements grants;

    String getID(char) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override;
};

}
