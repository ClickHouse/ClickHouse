#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/Common/AccessEntityType.h>


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
  * SHOW CREATE MASKING POLICY name ON [database.]table
  * SHOW CREATE MASKING POLICIES [name [, name2 ...] | name ON [database.]table | ON [database.]table]
  * SHOW CREATE QUOTA [name]
  * SHOW CREATE QUOTAS [name [, name2 ...]]
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    AccessEntityType type{};
    Strings names;
    boost::intrusive_ptr<ASTRowPolicyNames> row_policy_names;

    bool current_quota = false;
    bool current_user = false;
    bool all = false;

    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;

    String getID(char) const override;

    /// `getID` covers only `type` (and the singular/plural keyword), while `names`,
    /// `row_policy_names`, `short_name` and `database_and_table_name` are plain members outside
    /// `children`. Fold them into the hash so the rewrite-rule matcher, which treats an equal tree
    /// hash as semantic equality, does not over-match e.g. `SHOW CREATE USER a` and
    /// `SHOW CREATE USER b`.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr clone() const override;

    void replaceEmptyDatabase(const String & current_database);

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    String getKeyword() const;
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
