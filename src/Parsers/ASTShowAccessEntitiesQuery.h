#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/IAccessEntity.h>


namespace DB
{

/// SHOW [ROW] POLICIES [ON [database.]table]
/// SHOW QUOTAS
/// SHOW [CURRENT] QUOTA
/// SHOW [SETTINGS] PROFILES
/// SHOW USERS
/// SHOW [CURRENT|ENABLED] ROLES
class ASTShowAccessEntitiesQuery : public ASTQueryWithOutput
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;
    String database;
    String table_name;
    bool current_quota = false;
    bool current_roles = false;
    bool enabled_roles = false;

    String getID(char) const override;
    ASTPtr clone() const override { return std::make_shared<ASTShowAccessEntitiesQuery>(*this); }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    const char * getKeyword() const;
};

}
