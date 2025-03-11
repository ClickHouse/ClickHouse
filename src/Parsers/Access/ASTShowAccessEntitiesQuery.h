#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/Common/AccessEntityType.h>


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
    AccessEntityType type;

    bool all = false;
    bool current_quota = false;
    bool current_roles = false;
    bool enabled_roles = false;

    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;

    String getID(char) const override;
    ASTPtr clone() const override { return std::make_shared<ASTShowAccessEntitiesQuery>(*this); }

    void replaceEmptyDatabase(const String & current_database);

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String getKeyword() const;
};

}
