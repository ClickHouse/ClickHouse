#pragma once

#include <Access/IAccessEntity.h>
#include <Access/ExtendedRoleSet.h>


namespace DB
{
class Context;


/** Represents a row level security policy for a table.
  */
struct RowPolicy : public IAccessEntity
{
    void setDatabase(const String & database_);
    void setTableName(const String & table_name_);
    void setName(const String & policy_name_) override;
    void setFullName(const String & database_, const String & table_name_, const String & policy_name_);

    String getDatabase() const { return database; }
    String getTableName() const { return table_name; }
    String getName() const override { return policy_name; }

    struct FullNameParts
    {
        String database;
        String table_name;
        String policy_name;
        String getFullName() const;
        String getFullName(const Context & context) const;
    };

    /// Filter is a SQL conditional expression used to figure out which rows should be visible
    /// for user or available for modification. If the expression returns NULL or false for some rows
    /// those rows are silently suppressed.
    /// Check is a SQL condition expression used to check whether a row can be written into
    /// the table. If the expression returns NULL or false an exception is thrown.
    /// If a conditional expression here is empty it means no filtering is applied.
    enum ConditionType
    {
        SELECT_FILTER,
        INSERT_CHECK,
        UPDATE_FILTER,
        UPDATE_CHECK,
        DELETE_FILTER,

        MAX_CONDITION_TYPE
    };
    static const char * conditionTypeToString(ConditionType index);
    static const char * conditionTypeToColumnName(ConditionType index);

    String conditions[MAX_CONDITION_TYPE];

    /// Sets that the policy is permissive.
    /// A row is only accessible if at least one of the permissive policies passes,
    /// in addition to all the restrictive policies.
    void setPermissive(bool permissive_ = true) { setRestrictive(!permissive_); }
    bool isPermissive() const { return !isRestrictive(); }

    /// Sets that the policy is restrictive.
    /// A row is only accessible if at least one of the permissive policies passes,
    /// in addition to all the restrictive policies.
    void setRestrictive(bool restrictive_ = true) { restrictive = restrictive_; }
    bool isRestrictive() const { return restrictive; }

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<RowPolicy>(); }

    /// Which roles or users should use this row policy.
    ExtendedRoleSet to_roles;

private:
    String database;
    String table_name;
    String policy_name;
    bool restrictive = false;
};

using RowPolicyPtr = std::shared_ptr<const RowPolicy>;
}
