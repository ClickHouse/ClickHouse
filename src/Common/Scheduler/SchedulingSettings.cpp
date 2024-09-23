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

void SchedulingSettings::updateFromAST(const ASTPtr & settings, const String & resource_name)
{
    UNUSED(resource_name); // TODO(serxa): read resource specific settings from AST
    if (auto * set = typeid_cast<ASTSetQuery *>(settings.get()))
    {
        std::optional<Float64> new_weight;
        std::optional<Priority> new_priority;
        std::optional<Float64> new_max_speed;
        std::optional<Float64> new_max_burst;
        std::optional<Int64> new_max_requests;
        std::optional<Int64> new_max_cost;

        auto get_not_negative_float64 = [] (const String & name, const Field & field) {
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
        };

        auto get_not_negative_int64 = [] (const String & name, const Field & field) {
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
        };

        // Read changed setting values
        for (const auto & [name, value] : set->changes)
        {
            // TODO(serxa): we should validate workloads with this function before storing in WorkloadEntityStorage
            // TODO(serxa): and probably we should add and persist version in filename for future changes
            if (name == "weight")
                new_weight = get_not_negative_float64(name, value);
            else if (name == "priority")
                new_priority = Priority{value.safeGet<Priority::Value>()};
            else if (name == "max_speed")
                new_max_speed = get_not_negative_float64(name, value);
            else if (name == "max_burst")
                new_max_burst = get_not_negative_float64(name, value);
            else if (name == "max_requests")
                new_max_requests = get_not_negative_int64(name, value);
            else if (name == "max_cost")
                new_max_cost = get_not_negative_int64(name, value);
        }

        // Read setting to be reset to default values
        static SchedulingSettings default_settings;
        bool reset_max_burst = false;
        for (const String & name : set->default_settings)
        {
            if (name == "weight")
                new_weight = default_settings.weight;
            else if (name == "priority")
                new_priority = default_settings.priority;
            else if (name == "max_speed")
                new_max_speed = default_settings.max_speed;
            else if (name == "max_burst")
                reset_max_burst = true;
            else if (name == "max_requests")
                new_max_requests = default_settings.max_requests;
            else if (name == "max_cost")
                new_max_cost = default_settings.max_cost;
        }
        if (reset_max_burst)
            new_max_burst = default_burst_seconds * (new_max_speed ? *new_max_speed : max_speed);

        // Validate we could use values we read in a scheduler node
        {
            SchedulerNodeInfo validating_node(new_weight ? *new_weight : weight, new_priority ? *new_priority : priority);
        }

        // Save new values into the `this` object
        // Leave previous value intentionally for ALTER query to be able to skip not mentioned setting value
        if (new_weight)
            weight = *new_weight;
        if (new_priority)
            priority = *new_priority;
        if (new_max_speed)
        {
            max_speed = *new_max_speed;
            // We always set max_burst if max_speed is changed.
            // This is done for users to be able to ignore more advanced max_burst setting and rely only on max_speed
            if (!new_max_burst)
                max_burst = default_burst_seconds * max_speed;
        }
        if (new_max_burst)
            max_burst = *new_max_burst;
        if (new_max_requests)
            max_requests = *new_max_requests;
        if (new_max_cost)
            max_cost = *new_max_cost;
    }
}

}
