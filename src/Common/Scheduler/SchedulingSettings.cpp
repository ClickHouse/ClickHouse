#include <limits>
#include <Common/Scheduler/SchedulingSettings.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void SchedulingSettings::updateFromChanges(const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name)
{
    struct {
        std::optional<Float64> new_weight;
        std::optional<Priority> new_priority;
        std::optional<Float64> new_max_speed;
        std::optional<Float64> new_max_burst;
        std::optional<Int64> new_max_requests;
        std::optional<Int64> new_max_cost;

        static Float64 getNotNegativeFloat64(const String & name, const Field & field)
        {
            {
                UInt64 val;
                if (field.tryGet(val))
                    return static_cast<Float64>(val); // We dont mind slight loss of precision
            }

            {
                Int64 val;
                if (field.tryGet(val))
                {
                    if (val < 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected negative Int64 value for workload setting '{}'", name);
                    return static_cast<Float64>(val); // We dont mind slight loss of precision
                }
            }

            return field.safeGet<Float64>();
        }

        static Int64 getNotNegativeInt64(const String & name, const Field & field)
        {
            {
                UInt64 val;
                if (field.tryGet(val))
                {
                    // Saturate on overflow
                    if (val > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                        val = std::numeric_limits<Int64>::max();
                    return static_cast<Int64>(val);
                }
            }

            {
                Int64 val;
                if (field.tryGet(val))
                {
                    if (val < 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected negative Int64 value for workload setting '{}'", name);
                    return val;
                }
            }

            return field.safeGet<Int64>();
        }

        void read(const String & name, const Field & value)
        {
            if (name == "weight")
                new_weight = getNotNegativeFloat64(name, value);
            else if (name == "priority")
                new_priority = Priority{value.safeGet<Priority::Value>()};
            else if (name == "max_speed")
                new_max_speed = getNotNegativeFloat64(name, value);
            else if (name == "max_burst")
                new_max_burst = getNotNegativeFloat64(name, value);
            else if (name == "max_requests")
                new_max_requests = getNotNegativeInt64(name, value);
            else if (name == "max_cost")
                new_max_cost = getNotNegativeInt64(name, value);
        }
    } regular, specific;

    // Read changed setting values
    for (const auto & [name, value, resource] : changes)
    {
        if (resource.empty())
            regular.read(name, value);
        else if (resource == resource_name)
            specific.read(name, value);
    }

    auto get_value = [] <typename T> (const std::optional<T> & specific_new, const std::optional<T> & regular_new, T & old)
    {
        if (specific_new)
            return *specific_new;
        if (regular_new)
            return *regular_new;
        return old;
    };

    // Validate that we could use values read in a scheduler node
    {
        SchedulerNodeInfo validating_node(
            get_value(specific.new_weight, regular.new_weight, weight),
            get_value(specific.new_priority, regular.new_priority, priority));
    }

    // Commit new values.
    // Previous values are left intentionally for ALTER query to be able to skip not mentioned setting values
    weight = get_value(specific.new_weight, regular.new_weight, weight);
    priority = get_value(specific.new_priority, regular.new_priority, priority);
    if (specific.new_max_speed || regular.new_max_speed)
    {
        max_speed = get_value(specific.new_max_speed, regular.new_max_speed, max_speed);
        // We always set max_burst if max_speed is changed.
        // This is done for users to be able to ignore more advanced max_burst setting and rely only on max_speed
        max_burst = default_burst_seconds * max_speed;
    }
    max_burst = get_value(specific.new_max_burst, regular.new_max_burst, max_burst);
    max_requests = get_value(specific.new_max_requests, regular.new_max_requests, max_requests);
    max_cost = get_value(specific.new_max_cost, regular.new_max_cost, max_cost);
}

}
