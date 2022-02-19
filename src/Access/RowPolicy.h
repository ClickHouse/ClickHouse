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

    /// Sets the kind of the policy, it affects how row policies are applied.
    void setKind(RowPolicyKind kind_) { kind = kind_; }
    RowPolicyKind getKind() const { return kind; }

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<RowPolicy>(); }
    static constexpr const auto TYPE = AccessEntityType::ROW_POLICY;
    AccessEntityType getType() const override { return TYPE; }

    /// Which roles or users should use this row policy.
    RolesOrUsersSet to_roles;

private:
    void setName(const String &) override;

    RowPolicyName full_name;
    RowPolicyKind kind = RowPolicyKind::PERMISSIVE;
};

using RowPolicyPtr = std::shared_ptr<const RowPolicy>;

}
