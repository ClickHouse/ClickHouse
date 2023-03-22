#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** SHOW GRANTS [FOR user_name]
  */
class ASTShowGrantsQuery : public ASTQueryWithOutput
{
public:
    std::shared_ptr<ASTRolesOrUsersSet> for_roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatQueryImpl(FormattingBuffer out) const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }
};
}
