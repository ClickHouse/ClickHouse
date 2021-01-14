#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/IAccessEntity.h>


namespace DB
{
class ASTRowPolicyNames;

using Strings = std::vector<String>;

/** SHOW CREATE USER [name | CURRENT_USER]
  * SHOW CREATE USERS [name [, name2 ...]
  * SHOW CREATE ROLE name
  * SHOW CREATE ROLES [name [, name2 ...]]
  * SHOW CREATE [SETTINGS] PROFILE name
  * SHOW CREATE [SETTINGS] PROFILES [name [, name2 ...]]
  * SHOW CREATE [ROW] POLICY name ON [database.]table
  * SHOW CREATE [ROW] POLICIES [name ON [database.]table [, name2 ON database2.table2 ...] | name | ON database.table]
  * SHOW CREATE QUOTA [name]
  * SHOW CREATE QUOTAS [name [, name2 ...]]
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;
    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;

    bool current_quota = false;
    bool current_user = false;
    bool all = false;

    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;

    String getID(char) const override;
    ASTPtr clone() const override;

    void replaceEmptyDatabaseWithCurrent(const String & current_database);

protected:
    String getKeyword() const;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
