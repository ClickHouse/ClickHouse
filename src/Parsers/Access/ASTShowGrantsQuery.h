#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL] [FOR QUERY select_query]
  */
class ASTShowGrantsQuery : public ASTQueryWithOutput
{
public:
    boost::intrusive_ptr<ASTRolesOrUsersSet> for_roles;
    bool with_implicit = false;
    bool final = false;

    /// Kept in `children`.
    ASTPtr for_query;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }
};
}
