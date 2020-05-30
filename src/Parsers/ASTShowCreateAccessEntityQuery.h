#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/IAccessEntity.h>


namespace DB
{
class ASTRowPolicyNames;

/** SHOW CREATE QUOTA [name]
  * SHOW CREATE [ROW] POLICY name ON [database.]table
  * SHOW CREATE USER [name | CURRENT_USER]
  * SHOW CREATE ROLE name
  * SHOW CREATE [SETTINGS] PROFILE name
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;
    Strings names;
    bool current_quota = false;
    bool current_user = false;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;

    String getID(char) const override;
    ASTPtr clone() const override;

    void replaceEmptyDatabaseWithCurrent(const String & current_database);

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
