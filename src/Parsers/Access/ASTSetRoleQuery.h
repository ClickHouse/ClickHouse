#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
  * SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...] [ON CLUSTER cluster_name]
  */
class ASTSetRoleQuery : public IAST, public ASTQueryWithOnCluster
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

    QueryKind getQueryKind() const override { return QueryKind::Set; }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTSetRoleQuery>(clone());
    }

    void replaceCurrentUserTag(const String & current_user_name) const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
