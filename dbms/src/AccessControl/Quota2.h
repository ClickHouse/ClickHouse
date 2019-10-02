#pragma once

#include <AccessControl/IControlAttributesDriven.h>
#include <chrono>
#include <map>


namespace DB
{
class Role;


/// Quota for resources consumption for specific interval. Used to limit resource usage by user.
///
/// Syntax:
/// CREATE QUOTA [IF NOT EXISTS] name
///     {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
///     [KEYED BY USERNAME | KEYED BY IP | NOT KEYED]
///     [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
///     TO {rolename | username | CURRENT_USER | PUBLIC} [, ...]
///     EXCEPT {rolename | username | CURRENT_USER} [, ...]
///
/// ALTER QUOTA name
///     {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
///     [KEYED BY USERNAME | KEYED BY IP | NOT KEYED]
///     [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
///     TO {rolename | username | CURRENT_USER | PUBLIC} [, ...]
///     EXCEPT {rolename | username | CURRENT_USER} [, ...]
///
/// DROP QUOTA [IF EXISTS] name
///
/// SHOW CREATE QUOTA name
class Quota2 : public IControlAttributesDriven
{
public:
    static const Type TYPE;

    enum class ResourceType
    {
        QUERIES,             /// Number of queries.
        ERRORS,              /// Number of queries with exceptions.
        RESULT_ROWS,         /// Number of rows returned as result.
        RESULT_BYTES,        /// Number of bytes returned as result.
        READ_ROWS,           /// Number of rows read from tables.
        READ_BYTES,          /// Number of bytes read from tables.
        EXECUTION_TIME_USEC, /// Total amount of query execution time in microseconds.
    };

    static constexpr size_t MAX_RESOURCE_TYPE = 7;
    static String getResourceName(ResourceType resource_type);

    using ResourceAmount = size_t;

    struct Limits
    {
        ResourceAmount limits[MAX_RESOURCE_TYPE];

        Limits();
        ResourceAmount operator[](ResourceType resource_type) const { return limits[static_cast<size_t>(resource_type)]; }
        ResourceAmount & operator[](ResourceType resource_type) { return limits[static_cast<size_t>(resource_type)]; }

        friend bool operator ==(const Limits & lhs, const Limits & rhs);
        friend bool operator !=(const Limits & lhs, const Limits & rhs) { return !(lhs == rhs); }

        static const Limits UNLIMITED;
    };

    enum class KeyType
    {
        NOT_KEYED,  /// Resource usage is calculated in total.
        USER_NAME,  /// Resource usage is calculated for each user name separately.
        IP_ADDRESS, /// Resource usage is calculated for each IP address separately.
    };

    struct Attributes : public IAttributes
    {
        KeyType key_type = KeyType::NOT_KEYED;
        bool allow_custom_key = true;
        std::map<std::chrono::seconds, Limits> limits_for_duration;
        std::vector<UUID> roles_allowed_to_consume;
        std::vector<UUID> roles_disallowed_consume;

        const Type & getType() const override;
        std::shared_ptr<IAttributes> clone() const override;

    protected:
        bool equal(const IAttributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using IControlAttributesDriven::IControlAttributesDriven;

    AttributesPtr getAttributes() const;
    AttributesPtr tryGetAttributes() const;
    const Type & getType() const override;

    void setKeyType(KeyType key_type);
    Changes setKeyTypeChanges(KeyType key_type);
    KeyType getKeyType() const;

    void setAllowCustomKey(bool allow);
    Changes setAllowCustomKeyChanges(bool allow);
    bool isCustomKeyAllowed() const;

    void setLimits(const std::vector<std::pair<std::chrono::seconds, Limits>> & limits);
    Changes setLimitsChanges(const std::vector<std::pair<std::chrono::seconds, Limits>> & limits);
    std::vector<std::pair<std::chrono::seconds, Limits> getLimits() const;

    void setRolesAllowedToConsume(const std::vector<Role> & roles);
    Changes setRolesAllowedToConsume(const std::vector<Role> & roles);
    std::vector<Role> getRolesAllowedToConsume() const;

    void setRolesDisallowedToConsume(const std::vector<Role> & roles);
    Changes setRolesDisallowedToConsume(const std::vector<Role> & roles);
    std::vector<Role> getRolesDisallowedToConsume() const;
};
}
