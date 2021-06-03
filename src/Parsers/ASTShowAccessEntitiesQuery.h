#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/IAccessEntity.h>


namespace DB
{

/// SHOW USERS
/// SHOW [CURRENT|ENABLED] ROLES
/// SHOW [SETTINGS] PROFILES
/// SHOW [ROW] POLICIES [name | ON [database.]table]
/// SHOW QUOTAS
/// SHOW [CURRENT] QUOTA
class ASTShowAccessEntitiesQuery : public ASTQueryWithOutput
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;

    bool all = false;
    bool current_quota = false;
    bool current_roles = false;
    bool enabled_roles = false;

    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;

    String getID(char) const override;
    ASTPtr clone() const override { return std::make_shared<ASTShowAccessEntitiesQuery>(*this); }

    void replaceEmptyDatabaseWithCurrent(const String & current_database);

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String getKeyword() const;
};

}
