#pragma once

#include <ACL/IACLAttributable.h>
#include <chrono>
#include <map>


namespace DB
{
/// Quota for resources consumption for specific interval.
/// Used to limit resource usage by user.
class Quota2 : public IACLAttributable
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

    struct Attributes : public IACLAttributable::Attributes
    {
        ConsumptionKey consumption_key = ConsumptionKey::NONE;
        bool allow_custom_consumption_key = true;

        std::map<std::chrono::seconds, Limits> limits_for_duration;

        Type getType() const override { return Type::QUOTA; }
        std::shared_ptr<IACLAttributes> clone() const override;

    protected:
        bool equal(const IACLAttributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using IACLAttributable::IACLAttributable;

    AttributesPtr getAttributes() const { AttributesPtr attrs; readAttributes(attrs); return attrs; }
    AttributesPtr getAttributesStrict() const { AttributesPtr attrs; readAttributesStrict(attrs); return attrs; }

    void setConsumptionKey(ConsumptionKey consumption_key) { perform(setConsumptionKeyOp(consumption_key)); }
    Operation setConsumptionKeyOp(ConsumptionKey consumption_key) const;
    ConsumptionKey getConsumptionKey() const;

    void setAllowCustomConsumptionKey(bool allow) { perform(setAllowCustomConsumptionKeyOp(allow)); }
    Operation setAllowCustomConsumptionKeyOp(bool allow) const;
    bool isCustomConsumptionKeyAllowed() const;

    void setLimitForDuration(std::chrono::seconds duration, ResourceType resource_type, ResourceAmount new_limit) { perform(setLimitForDurationOp(duration, resource_type, new_limit)); }
    Operation setLimitForDurationOp(std::chrono::seconds duration, ResourceType resource_type, ResourceAmount new_limit) const;
    std::vector<std::pair<std::chrono::seconds, ResourceAmount>> getLimits(ResourceType resource_type) const;

private:
    const String & getTypeName() const override;
    int getNotFoundErrorCode() const override;
    int getAlreadyExistsErrorCode() const override;
};
}
