#pragma once

#include <Parsers/IAST.h>
#include <Access/AccessRightsElement.h>


namespace DB
{
class ASTGenericRoleSet;


/** GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user_name | CURRENT_USER} [,...] [WITH GRANT OPTION]
  * REVOKE access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | CURRENT_USER} [,...]
  *
  * GRANT role [,...] TO {user_name | role_name | CURRENT_USER} [,...] [WITH ADMIN OPTION]
  * REVOKE [ADMIN OPTION FOR] role [,...] FROM {user_name | role_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
  */
class ASTGrantQuery : public IAST
{
public:
    enum class Kind
    {
        GRANT,
        REVOKE,
    };
    Kind kind = Kind::GRANT;
    AccessRightsElements access_rights_elements;
    std::shared_ptr<ASTGenericRoleSet> roles;
    std::shared_ptr<ASTGenericRoleSet> to_roles;
    bool grant_option = false;
    bool admin_option = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
