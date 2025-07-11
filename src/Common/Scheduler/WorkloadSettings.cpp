#include <limits>
#include <Common/Scheduler/CostUnit.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool WorkloadSettings::hasThrottler() const
{
    switch (unit) {
        case CostUnit::IOByte: return max_bytes_per_second != 0;
        case CostUnit::CPUSlot: return false;
        case CostUnit::QuerySlot: return max_queries_per_second != 0;
    }
}

Float64 WorkloadSettings::getThrottlerMaxSpeed() const
{
    switch (unit) {
        case CostUnit::IOByte: return max_bytes_per_second;
        case CostUnit::CPUSlot: return 0;
        case CostUnit::QuerySlot: return max_queries_per_second;
    }
}

Float64 WorkloadSettings::getThrottlerMaxBurst() const
{
    switch (unit) {
        case CostUnit::IOByte: return max_burst_bytes;
        case CostUnit::CPUSlot: return 0;
        case CostUnit::QuerySlot: return max_burst_queries;
    }
}

bool WorkloadSettings::hasSemaphore() const
{
    switch (unit) {
        case CostUnit::IOByte: return max_io_requests != unlimited || max_bytes_inflight != unlimited;
        case CostUnit::CPUSlot: return max_concurrent_threads != unlimited;
        case CostUnit::QuerySlot: return max_concurrent_queries != unlimited;
    }
}

Int64 WorkloadSettings::getSemaphoreMaxRequests() const
{
    switch (unit) {
        case CostUnit::IOByte: return max_io_requests;
        case CostUnit::CPUSlot: return max_concurrent_threads;
        case CostUnit::QuerySlot: return max_concurrent_queries;
    }
}

Int64 WorkloadSettings::getSemaphoreMaxCost() const
{
    switch (unit) {
        case CostUnit::IOByte: return max_bytes_inflight;
        case CostUnit::CPUSlot: return unlimited;
        case CostUnit::QuerySlot: return unlimited;
    }
}

void WorkloadSettings::initFromChanges(CostUnit unit_, const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name, bool throw_on_unknown_setting)
{
    // Set resource unit
    unit = unit_;

    struct {
        std::optional<Float64> weight;
        std::optional<Priority> priority;
        std::optional<Float64> max_bytes_per_second;
        std::optional<Float64> max_burst_bytes;
        std::optional<Float64> max_queries_per_second;
        std::optional<Float64> max_burst_queries;
        std::optional<Int64> max_io_requests;
        std::optional<Int64> max_bytes_inflight;
        std::optional<Int64> max_concurrent_threads;
        std::optional<Float64> max_concurrent_threads_ratio_to_cores;
        std::optional<Int64> max_concurrent_queries;

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

        void read(const String & name, const Field & value, bool throw_on_unknown_setting)
        {
            // Note that the second workload setting name options are provided for backward-compatibility
            if (name == "weight")
                weight = getNotNegativeFloat64(name, value);
            else if (name == "priority")
                priority = Priority{value.safeGet<Priority::Value>()};
            else if (name == "max_bytes_per_second" || name == "max_speed")
                max_bytes_per_second = getNotNegativeFloat64(name, value);
            else if (name == "max_burst_bytes" || name == "max_burst")
                max_burst_bytes = getNotNegativeFloat64(name, value);
            else if (name == "max_queries_per_second")
                max_queries_per_second = getNotNegativeFloat64(name, value);
            else if (name == "max_burst_queries")
                max_burst_queries = getNotNegativeFloat64(name, value);
            else if (name == "max_io_requests" || name == "max_requests")
                max_io_requests = getNotNegativeInt64(name, value);
            else if (name == "max_bytes_inflight" || name == "max_cost")
                max_bytes_inflight = getNotNegativeInt64(name, value);
            else if (name == "max_concurrent_threads")
                max_concurrent_threads = getNotNegativeInt64(name, value);
            else if (name == "max_concurrent_threads_ratio_to_cores")
                max_concurrent_threads_ratio_to_cores = getNotNegativeFloat64(name, value);
            else if (name == "max_concurrent_queries")
                max_concurrent_queries = getNotNegativeInt64(name, value);
            else if (throw_on_unknown_setting)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown workload setting '{}'", name);
        }
    } regular, specific;

    // Read changed setting values
    for (const auto & [name, value, resource] : changes)
    {
        if (resource.empty())
            regular.read(name, value, throw_on_unknown_setting);
        else if (resource == resource_name)
            specific.read(name, value, throw_on_unknown_setting);
    }

    // Regular values are used for all resources, while specific values are from setting having 'FOR resource' clause and has priority over regular
    auto get_value = [] <typename T> (const std::optional<T> & specific_value, const std::optional<T> & regular_value, T default_value, T zero_value = T())
    {
        if (specific_value)
            return *specific_value == T() ? zero_value : *specific_value;
        if (regular_value)
            return *regular_value == T() ? zero_value : *regular_value;
        return default_value;
    };

    // Validate that we could use values read in a scheduler node
    {
        SchedulerNodeInfo validating_node(
            get_value(specific.weight, regular.weight, weight),
            get_value(specific.priority, regular.priority, priority));
    }

    // Choose values for given resource.
    // NOTE: previous values contain defaults.
    weight = get_value(specific.weight, regular.weight, weight);
    priority = get_value(specific.priority, regular.priority, priority);

    // IO throttling
    if (specific.max_bytes_per_second || regular.max_bytes_per_second)
    {
        max_bytes_per_second = get_value(specific.max_bytes_per_second, regular.max_bytes_per_second, max_bytes_per_second);
        // We always set max_burst_bytes if max_bytes_per_second is changed.
        // This is done for users to be able to ignore more advanced max_burst_bytes setting and rely only on max_bytes_per_second.
        max_burst_bytes = default_burst_seconds * max_bytes_per_second;
    }
    max_burst_bytes = get_value(specific.max_burst_bytes, regular.max_burst_bytes, max_burst_bytes);

    // Query throttling
    if (specific.max_queries_per_second || regular.max_queries_per_second)
    {
        max_queries_per_second = get_value(specific.max_queries_per_second, regular.max_queries_per_second, max_queries_per_second);
        // We always set max_burst_queries if max_queries_per_second is changed.
        // This is done for users to be able to ignore more advanced max_burst_queries setting and rely only on max_queries_per_second.
        max_burst_queries = default_burst_seconds * max_queries_per_second;
    }
    max_burst_queries = get_value(specific.max_burst_queries, regular.max_burst_queries, max_burst_queries);

    // Choose semaphore constraint values.
    // Zero setting value means unlimited number of requests or bytes.
    max_io_requests = get_value(specific.max_io_requests, regular.max_io_requests, max_io_requests, unlimited);
    max_bytes_inflight = get_value(specific.max_bytes_inflight, regular.max_bytes_inflight, max_bytes_inflight, unlimited);

    // Compute concurrent thread limit as minimum of two possible values: (1) exact limit and (2) ratio to cores limit.
    // Zero setting value means unlimited number of threads.
    Int64 limit = unlimited;
    Int64 exact_number = get_value(specific.max_concurrent_threads, regular.max_concurrent_threads, Int64(0));
    Float64 ratio_to_cores = get_value(specific.max_concurrent_threads_ratio_to_cores, regular.max_concurrent_threads_ratio_to_cores, 0.0);
    if (exact_number > 0 && exact_number < limit)
        limit = exact_number;
    if (ratio_to_cores > 0)
    {
        Int64 value = static_cast<Int64>(ratio_to_cores * getNumberOfCPUCoresToUse());
        if (value > 0 && value < limit)
            limit = value;
    }
    max_concurrent_threads = limit;

    // Concurrent queries limit
    max_concurrent_queries = get_value(specific.max_concurrent_queries, regular.max_concurrent_queries, max_concurrent_queries, unlimited);
}

}
