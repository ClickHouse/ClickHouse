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

bool SchedulingSettings::hasThrottler() const
{
    switch (unit) {
        case Unit::IOByte: return max_bytes_per_second != 0;
        case Unit::CPUSlot: return false;
    }
}

Float64 SchedulingSettings::getThrottlerMaxSpeed() const
{
    switch (unit) {
        case Unit::IOByte: return max_bytes_per_second;
        case Unit::CPUSlot: return 0;
    }
}

Float64 SchedulingSettings::getThrottlerMaxBurst() const
{
    switch (unit) {
        case Unit::IOByte: return max_burst_bytes;
        case Unit::CPUSlot: return 0;
    }
}

bool SchedulingSettings::hasSemaphore() const
{
    switch (unit) {
        case Unit::IOByte: return max_io_requests != unlimited || max_bytes_inflight != unlimited;
        case Unit::CPUSlot: return max_concurrent_threads != unlimited;
    }
}

Int64 SchedulingSettings::getSemaphoreMaxRequests() const
{
    switch (unit) {
        case Unit::IOByte: return max_io_requests;
        case Unit::CPUSlot: return max_concurrent_threads;
    }
}

Int64 SchedulingSettings::getSemaphoreMaxCost() const
{
    switch (unit) {
        case Unit::IOByte: return max_bytes_inflight;
        case Unit::CPUSlot: return unlimited;
    }
}

void SchedulingSettings::updateFromChanges(Unit unit_, const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name)
{
    // Set resource unit
    unit = unit_;

    struct {
        std::optional<Float64> new_weight;
        std::optional<Priority> new_priority;
        std::optional<Float64> new_max_bytes_per_second;
        std::optional<Float64> new_max_burst_bytes;
        std::optional<Int64> new_max_io_requests;
        std::optional<Int64> new_max_bytes_inflight;
        std::optional<Int64> new_max_concurrent_threads;

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
            // Note that the second workload setting name options are provided for backward-compatibility
            if (name == "weight")
                new_weight = getNotNegativeFloat64(name, value);
            else if (name == "priority")
                new_priority = Priority{value.safeGet<Priority::Value>()};
            else if (name == "max_bytes_per_second" || name == "max_speed")
                new_max_bytes_per_second = getNotNegativeFloat64(name, value);
            else if (name == "max_burst_bytes" || name == "max_burst")
                new_max_burst_bytes = getNotNegativeFloat64(name, value);
            else if (name == "max_io_requests" || name == "max_requests")
                new_max_io_requests = getNotNegativeInt64(name, value);
            else if (name == "max_bytes_inflight" || name == "max_cost")
                new_max_bytes_inflight = getNotNegativeInt64(name, value);
            else if (name == "max_concurrent_threads")
                new_max_concurrent_threads = getNotNegativeInt64(name, value);
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
    if (specific.new_max_bytes_per_second || regular.new_max_bytes_per_second)
    {
        max_bytes_per_second = get_value(specific.new_max_bytes_per_second, regular.new_max_bytes_per_second, max_bytes_per_second);
        // We always set max_burst_bytes if max_bytes_per_second is changed.
        // This is done for users to be able to ignore more advanced max_burst_bytes setting and rely only on max_bytes_per_second
        max_burst_bytes = default_burst_seconds * max_bytes_per_second;
    }
    max_burst_bytes = get_value(specific.new_max_burst_bytes, regular.new_max_burst_bytes, max_burst_bytes);
    max_io_requests = get_value(specific.new_max_io_requests, regular.new_max_io_requests, max_io_requests);
    max_bytes_inflight = get_value(specific.new_max_bytes_inflight, regular.new_max_bytes_inflight, max_bytes_inflight);
}

}
