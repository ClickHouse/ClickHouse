#pragma once

#include <Parsers/IAST.h>


namespace DB
{
class ASTGenericRoleSet;

/** SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
  * SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
  */
class ASTSetRoleQuery : public IAST
{
public:
    enum class Kind
    {
        SET_ROLE,
        SET_ROLE_DEFAULT,
        SET_DEFAULT_ROLE,
    };
    Kind kind = Kind::SET_ROLE;

    std::shared_ptr<ASTGenericRoleSet> roles;
    std::shared_ptr<ASTGenericRoleSet> to_users;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
