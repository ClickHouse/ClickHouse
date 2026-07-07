#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/Common/AccessEntityType.h>


namespace DB
{

/// SHOW USERS
/// SHOW [CURRENT|ENABLED] ROLES
/// SHOW [SETTINGS] PROFILES
/// SHOW [ROW] POLICIES [name | ON [database.]table]
/// SHOW MASKING POLICIES [name | ON [database.]table]
/// SHOW QUOTAS
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

    String getID(char) const override;

    /// `getID` covers only `type` and the `CURRENT` / `ENABLED` flags (through the keyword), while
    /// `short_name` and `database_and_table_name` are plain members outside `children`. Fold them
    /// into the hash so the rewrite-rule matcher, which treats an equal tree hash as semantic
    /// equality, does not over-match e.g. `SHOW ROW POLICIES p1` and `SHOW ROW POLICIES p2`.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr clone() const override { return make_intrusive<ASTShowAccessEntitiesQuery>(*this); }

    void replaceEmptyDatabase(const String & current_database);

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String getKeyword() const;
};

}
