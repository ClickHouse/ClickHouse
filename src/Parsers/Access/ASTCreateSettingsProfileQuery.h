#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{
class ASTSettingsProfileElements;
class ASTAlterSettingsProfileElements;
class ASTRolesOrUsersSet;


/** CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
  *     [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER SETTINGS PROFILE [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
  *     [DROP SETTINGS variable [,...] ]
  *     [ADD PROFILES 'profile_name' [,...] ]
  *     [DROP PROFILES 'profile_name' [,...] ]
  *     [DROP ALL PROFILES]
  *     [DROP ALL SETTINGS]
  *     [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ASTCreateSettingsProfileQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    String storage_name;

    Strings names;
    String new_name;

    std::shared_ptr<ASTSettingsProfileElements> settings;
    std::shared_ptr<ASTAlterSettingsProfileElements> alter_settings;

    std::shared_ptr<ASTRolesOrUsersSet> to_roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void replaceCurrentUserTag(const String & current_user_name) const;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateSettingsProfileQuery>(clone()); }
    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

}
