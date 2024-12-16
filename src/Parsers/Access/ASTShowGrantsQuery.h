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
    std::shared_ptr<ASTRolesOrUsersSet> for_roles;
    bool with_implicit = false;
    bool final = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }
};
}
