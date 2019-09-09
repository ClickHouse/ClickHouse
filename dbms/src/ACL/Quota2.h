#pragma once

#include <ACL/ACLAttributable.h>
#include <chrono>
#include <map>


namespace DB
{
/// Quota for resources consumption for specific interval.
/// Used to limit resource usage by user.
class Quota2 : public ACLAttributable
{
public:
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
    };

    static const Limits NoLimits;

    enum class ConsumptionKey
    {
        NONE,       /// Resource usage is calculated in total.
        USER_NAME,  /// Resource usage is calculated for each user name separately.
        IP_ADDRESS, /// Resource usage is calculated for each IP address separately.
    };

    struct Attributes : public ACLAttributable::Attributes
    {
        ConsumptionKey consumption_key = ConsumptionKey::NONE;
        bool allow_custom_consumption_key = true;

        std::map<std::chrono::seconds, Limits> limits_for_duration;

        ACLAttributesType getType() const override;
        std::shared_ptr<IACLAttributes> clone() const override;

    protected:
        bool equal(const IACLAttributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using ACLAttributable::ACLAttributable;

    AttributesPtr getAttributes() const;
    AttributesPtr getAttributesStrict() const;

    void setConsumptionKey(ConsumptionKey consumption_key);
    Operation setConsumptionKeyOp(ConsumptionKey consumption_key);
    ConsumptionKey getConsumptionKey() const;

    void setAllowCustomConsumptionKey(bool allow);
    Operation setAllowCustomConsumptionKeyOp(bool allow);
    bool isCustomConsumptionKeyAllowed() const;

    void setLimitForDuration(std::chrono::seconds duration, ResourceType resource_type, ResourceAmount new_limit);
    Operation setLimitForDurationOp(std::chrono::seconds duration, ResourceType resource_type, ResourceAmount new_limit);
    std::vector<std::pair<std::chrono::seconds, ResourceAmount>> getLimits(ResourceType resource_type) const;

private:
    ACLAttributesType getType() const override;
};
}
