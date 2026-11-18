#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Core/Types.h>
#include <array>


namespace DB
{

/** Represents a row level security policy for a table.
  */
struct RowPolicy : public IAccessEntity
{
    void setShortName(const String & short_name);
    void setDatabase(const String & database);
    void setTableName(const String & table_name);
    void setFullName(const String & short_name, const String & database, const String & table_name);
    void setFullName(const RowPolicyName & full_name_);

    const String & getDatabase() const { return full_name.database; }
    const String & getTableName() const { return full_name.table_name; }
    const String & getShortName() const { return full_name.short_name; }
    const RowPolicyName & getFullName() const { return full_name; }

    /// A SQL conditional expression used to figure out which rows should be visible
    /// for user or available for modification.
    std::array<String, static_cast<size_t>(RowPolicyFilterType::MAX)> filters;

    /// Sets that the policy is permissive.
    /// A row is only accessible if at least one of the permissive policies passes,
    /// in addition to all the restrictive policies.
    void setPermissive(bool permissive_ = true) { setRestrictive(!permissive_); }
    bool isPermissive() const { return !isRestrictive(); }

    /// Applied for entire database
    bool isForDatabase() const { return full_name.table_name == RowPolicyName::ANY_TABLE_MARK; }

    /// Sets that the policy is restrictive.
    /// A row is only accessible if at least one of the permissive policies passes,
    /// in addition to all the restrictive policies.
    void setRestrictive(bool restrictive_ = true) { restrictive = restrictive_; }
    bool isRestrictive() const { return restrictive; }

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<RowPolicy>(); }
    static constexpr const auto TYPE = AccessEntityType::ROW_POLICY;
    AccessEntityType getType() const override { return TYPE; }

    std::vector<UUID> findDependencies() const override;
    bool hasDependencies(const std::unordered_set<UUID> & ids) const override;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    void copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids) override;
    void removeDependencies(const std::unordered_set<UUID> & ids) override;
    void clearAllExceptDependencies() override;

    bool isBackupAllowed() const override { return true; }

    /// Which roles or users should use this row policy.
    RolesOrUsersSet to_roles;

private:
    void setName(const String &) override;

    RowPolicyName full_name;
    bool restrictive = false;
};

using RowPolicyPtr = std::shared_ptr<const RowPolicy>;

}
