#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <Core/Types.h>
#include <memory>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;

/// Represents the full name of a masking policy, e.g. "mask_email ON mydb.mytable".
struct MaskingPolicyName
{
    String short_name;
    String database;
    String table_name;

    bool empty() const { return short_name.empty(); }
    String toString() const
    {
        return backQuoteIfNeed(short_name) + " ON " + (database.empty() ? String() : backQuoteIfNeed(database) + ".") + backQuoteIfNeed(table_name);
    }
    auto toTuple() const { return std::tie(short_name, database, table_name); }
    friend bool operator ==(const MaskingPolicyName & left, const MaskingPolicyName & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator !=(const MaskingPolicyName & left, const MaskingPolicyName & right) { return left.toTuple() != right.toTuple(); }
};


/** Represents a data masking policy for a table.
  */
struct MaskingPolicy : public IAccessEntity
{
    void setShortName(const String & short_name);
    void setDatabase(const String & database);
    void setTableName(const String & table_name);
    void setFullName(const String & short_name, const String & database, const String & table_name);
    void setFullName(const MaskingPolicyName & full_name_);

    const String & getDatabase() const { return full_name.database; }
    const String & getTableName() const { return full_name.table_name; }
    const String & getShortName() const { return full_name.short_name; }
    const MaskingPolicyName & getFullName() const { return full_name; }

    /// UPDATE assignments (ASTExpressionList of ASTAssignment objects)
    ASTPtr update_assignments;

    /// Optional WHERE condition to apply the mask selectively
    ASTPtr where_condition;

    /// Priority for applying multiple masking policies (higher priority is applied first)
    Int64 priority = 0;

    /// Which roles or users should use this masking policy.
    RolesOrUsersSet to_roles;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<MaskingPolicy>(); }
    static constexpr auto TYPE = AccessEntityType::MASKING_POLICY;
    AccessEntityType getType() const override { return TYPE; }

    std::vector<UUID> findDependencies() const override;
    bool hasDependencies(const std::unordered_set<UUID> & ids) const override;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    void copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids) override;
    void removeDependencies(const std::unordered_set<UUID> & ids) override;
    void clearAllExceptDependencies() override;

    bool isBackupAllowed() const override { return true; }

private:
    void setName(const String &) override;

    MaskingPolicyName full_name;
};

using MaskingPolicyPtr = std::shared_ptr<const MaskingPolicy>;

}
