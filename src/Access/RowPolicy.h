#pragma once

#include <Access/IAccessEntity.h>
#include <Access/ExtendedRoleSet.h>


namespace DB
{

/** Represents a row level security policy for a table.
  */
struct RowPolicy : public IAccessEntity
{
    struct NameParts
    {
        String short_name;
        String database;
        String table_name;

        String getName() const;
        auto toTuple() const { return std::tie(short_name, database, table_name); }
        friend bool operator ==(const NameParts & left, const NameParts & right) { return left.toTuple() == right.toTuple(); }
        friend bool operator !=(const NameParts & left, const NameParts & right) { return left.toTuple() != right.toTuple(); }
    };

    void setShortName(const String & short_name);
    void setDatabase(const String & database);
    void setTableName(const String & table_name);
    void setNameParts(const String & short_name, const String & database, const String & table_name);
    void setNameParts(const NameParts & name_parts);

    const String & getDatabase() const { return name_parts.database; }
    const String & getTableName() const { return name_parts.table_name; }
    const String & getShortName() const { return name_parts.short_name; }
    const NameParts & getNameParts() const { return name_parts; }

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
    static constexpr const Type TYPE = Type::ROW_POLICY;
    Type getType() const override { return TYPE; }

    /// Which roles or users should use this row policy.
    ExtendedRoleSet to_roles;

private:
    void setName(const String & name_) override;

    NameParts name_parts;
    bool restrictive = false;
};

using RowPolicyPtr = std::shared_ptr<const RowPolicy>;
}
