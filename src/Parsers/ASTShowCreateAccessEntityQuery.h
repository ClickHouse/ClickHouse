#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/RowPolicy.h>


namespace DB
{
/** SHOW CREATE QUOTA [name | CURRENT]
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
    String name;
    bool current_quota = false;
    bool current_user = false;
    RowPolicy::NameParts row_policy_name_parts;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
