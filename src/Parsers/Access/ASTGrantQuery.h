#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{
class ASTRolesOrUsersSet;


/** GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user_name | CURRENT_USER} [,...] [WITH GRANT OPTION]
  * REVOKE access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | CURRENT_USER} [,...]
  *
  * GRANT role [,...] TO {user_name | role_name | CURRENT_USER} [,...] [WITH ADMIN OPTION]
  * REVOKE [ADMIN OPTION FOR] role [,...] FROM {user_name | role_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
  */
class ASTGrantQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool attach_mode = false;
    bool is_revoke = false;
    AccessRightsElements access_rights_elements;
    std::shared_ptr<ASTRolesOrUsersSet> roles;
    bool admin_option = false;
    bool replace_access = false;
    bool replace_granted_roles = false;
    std::shared_ptr<ASTRolesOrUsersSet> grantees;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void replaceEmptyDatabase(const String & current_database);
    void replaceCurrentUserTag(const String & current_user_name) const;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTGrantQuery>(clone()); }
    QueryKind getQueryKind() const override { return is_revoke ? QueryKind::Revoke : QueryKind::Grant; }
};
}
