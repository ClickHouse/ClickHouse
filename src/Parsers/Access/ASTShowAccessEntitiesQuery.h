#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/Common/AccessEntityType.h>


namespace DB
{

/// SHOW USERS [[NOT] [I]LIKE 'pattern']
/// SHOW [CURRENT|ENABLED] ROLES [[NOT] [I]LIKE 'pattern']
/// SHOW [SETTINGS] PROFILES [[NOT] [I]LIKE 'pattern']
/// SHOW [ROW] POLICIES [name | ON [database.]table] [[NOT] [I]LIKE 'pattern']
/// SHOW MASKING POLICIES [name | ON [database.]table] [[NOT] [I]LIKE 'pattern']
/// SHOW QUOTAS [[NOT] [I]LIKE 'pattern']
/// SHOW [CURRENT] QUOTA
class ASTShowAccessEntitiesQuery : public ASTQueryWithOutput
{
public:
    AccessEntityType type{};

    bool all = false;
    bool current_quota = false;
    bool current_roles = false;
    bool enabled_roles = false;

    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;

    String like;
    bool not_like = false;
    bool case_insensitive_like = false;

    String getID(char) const override;
    ASTPtr clone() const override { return make_intrusive<ASTShowAccessEntitiesQuery>(*this); }

    void replaceEmptyDatabase(const String & current_database);

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String getKeyword() const;
};

}
