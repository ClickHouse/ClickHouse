#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/RowPolicy.h>
#include <optional>


namespace DB
{
class ASTRowPolicyNames;
class ASTRolesOrUsersSet;

/** CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] name ON [database.]table
  *      [AS {permissive | restrictive}]
  *      [FOR {SELECT | INSERT | UPDATE | DELETE | ALL}]
  *      [USING condition]
  *      [WITH CHECK condition] [,...]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER [ROW] POLICY [IF EXISTS] name ON [database.]table
  *      [RENAME TO new_name]
  *      [AS {permissive | restrictive}]
  *      [FOR {SELECT | INSERT | UPDATE | DELETE | ALL}]
  *      [USING {condition | NONE}]
  *      [WITH CHECK {condition | NONE}] [,...]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ASTCreateRowPolicyQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    std::shared_ptr<ASTRowPolicyNames> names;
    String new_short_name;

    std::optional<bool> is_restrictive;
    std::vector<std::pair<RowPolicy::ConditionType, ASTPtr>> conditions; /// `nullptr` means set to NONE.

    std::shared_ptr<ASTRolesOrUsersSet> roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateRowPolicyQuery>(clone()); }

    void replaceCurrentUserTagWithName(const String & current_user_name) const;
    void replaceEmptyDatabaseWithCurrent(const String & current_database) const;
};
}
