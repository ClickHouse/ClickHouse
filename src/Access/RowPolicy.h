#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <array>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** Represents a row level security policy for a table.
  */
struct RowPolicy : public IAccessEntity
{
    struct NameParts
    {
        String short_name;
        String database;
        String table_name;

        bool empty() const { return short_name.empty(); }
        String getName() const;
        String toString() const { return getName(); }
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

#if 0 /// Row-level security for INSERT, UPDATE, DELETE is not implemented yet.
        INSERT_CHECK,
        UPDATE_FILTER,
        UPDATE_CHECK,
        DELETE_FILTER,
#endif

        MAX_CONDITION_TYPE
    };

    struct ConditionTypeInfo
    {
        const char * const raw_name;
        const String name;    /// Lowercased with underscores, e.g. "select_filter".
        const String command; /// Uppercased without last word, e.g. "SELECT".
        const bool is_check;  /// E.g. false for SELECT_FILTER.
        static const ConditionTypeInfo & get(ConditionType type);
    };

    std::array<String, MAX_CONDITION_TYPE> conditions;

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
    RolesOrUsersSet to_roles;

private:
    void setName(const String & name_) override;

    NameParts name_parts;
    bool restrictive = false;
};

using RowPolicyPtr = std::shared_ptr<const RowPolicy>;


inline const RowPolicy::ConditionTypeInfo & RowPolicy::ConditionTypeInfo::get(ConditionType type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        size_t underscore_pos = init_name.find('_');
        String init_command = init_name.substr(0, underscore_pos);
        boost::to_upper(init_command);
        bool init_is_check = (std::string_view{init_name}.substr(underscore_pos + 1) == "check");
        return ConditionTypeInfo{raw_name_, std::move(init_name), std::move(init_command), init_is_check};
    };

    switch (type_)
    {
        case SELECT_FILTER:
        {
            static const ConditionTypeInfo info = make_info("SELECT_FILTER");
            return info;
        }
#if 0 /// Row-level security for INSERT, UPDATE, DELETE is not implemented yet.
        case INSERT_CHECK:
        {
            static const ConditionTypeInfo info = make_info("INSERT_CHECK");
            return info;
        }
        case UPDATE_FILTER:
        {
            static const ConditionTypeInfo info = make_info("UPDATE_FILTER");
            return info;
        }
        case UPDATE_CHECK:
        {
            static const ConditionTypeInfo info = make_info("UPDATE_CHECK");
            return info;
        }
        case DELETE_FILTER:
        {
            static const ConditionTypeInfo info = make_info("DELETE_FILTER");
            return info;
        }
#endif
        case MAX_CONDITION_TYPE: break;
    }
    throw Exception("Unknown type: " + std::to_string(static_cast<size_t>(type_)), ErrorCodes::LOGICAL_ERROR);
}

inline String toString(RowPolicy::ConditionType type)
{
    return RowPolicy::ConditionTypeInfo::get(type).raw_name;
}


inline String RowPolicy::NameParts::getName() const
{
    String name;
    name.reserve(database.length() + table_name.length() + short_name.length() + 6);
    name += backQuoteIfNeed(short_name);
    name += " ON ";
    if (!database.empty())
    {
        name += backQuoteIfNeed(database);
        name += '.';
    }
    name += backQuoteIfNeed(table_name);
    return name;
}

}
