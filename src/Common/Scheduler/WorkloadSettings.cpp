#include <limits>
#include <Common/Scheduler/CostUnit.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Parsers/ASTSetQuery.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_NUMBER;
}

bool WorkloadSettings::hasThrottler(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte: return max_bytes_per_second != 0;
        case CostUnit::CPUNanosecond: return max_cpus != 0;
        case CostUnit::QuerySlot: return max_queries_per_second != 0;
        case CostUnit::MemoryByte: chassert(false); return false;
    }
}

Float64 WorkloadSettings::getThrottlerMaxSpeed(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte: return max_bytes_per_second;
        case CostUnit::CPUNanosecond: return max_cpus * 1'000'000'000; // Convert CPU count to CPU * nanoseconds / sec
        case CostUnit::QuerySlot: return max_queries_per_second;
        case CostUnit::MemoryByte: chassert(false); return 0;
    }
}

Float64 WorkloadSettings::getThrottlerMaxBurst(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte: return max_burst_bytes;
        case CostUnit::CPUNanosecond: return max_burst_cpu_seconds * 1'000'000'000; // Convert seconds to nanoseconds
        case CostUnit::QuerySlot: return max_burst_queries;
        case CostUnit::MemoryByte: chassert(false); return 0;
    }
}

bool WorkloadSettings::hasSemaphore(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte: return max_io_requests != unlimited || max_bytes_inflight != unlimited;
        case CostUnit::CPUNanosecond: return max_concurrent_threads != unlimited;
        case CostUnit::QuerySlot: return max_concurrent_queries != unlimited;
        case CostUnit::MemoryByte: chassert(false); return false;
    }
}

Int64 WorkloadSettings::getSemaphoreMaxRequests(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte: return max_io_requests;
        case CostUnit::CPUNanosecond: return max_concurrent_threads;
        case CostUnit::QuerySlot: return max_concurrent_queries;
        case CostUnit::MemoryByte: chassert(false); return 0;
    }
}

Int64 WorkloadSettings::getSemaphoreMaxCost(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte: return max_bytes_inflight;
        case CostUnit::CPUNanosecond: return unlimited;
        case CostUnit::QuerySlot: return unlimited;
        case CostUnit::MemoryByte: chassert(false); return 0;
    }
}

bool WorkloadSettings::hasQueueLimit(CostUnit unit) const
{
    switch (unit) {
        // These requests are executed in the middle of query processing - queue is unlimited
        case CostUnit::IOByte:
        case CostUnit::CPUNanosecond:
            return false;
        case CostUnit::QuerySlot: return max_waiting_queries != unlimited;
        case CostUnit::MemoryByte: return max_waiting_queries != unlimited;
    }
}

Int64 WorkloadSettings::getQueueLimit(CostUnit unit) const
{
    switch (unit) {
        // These requests are executed in the middle of query processing - queue is unlimited
        case CostUnit::IOByte:
        case CostUnit::CPUNanosecond:
            return unlimited;
        case CostUnit::QuerySlot: return max_waiting_queries;
        case CostUnit::MemoryByte: return max_waiting_queries;
    }
}

bool WorkloadSettings::hasAllocationLimit(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte:
        case CostUnit::CPUNanosecond:
        case CostUnit::QuerySlot:
        case CostUnit::MemoryByte: return max_memory != unlimited;
    }
}

Int64 WorkloadSettings::getAllocationLimit(CostUnit unit) const
{
    switch (unit) {
        case CostUnit::IOByte:
        case CostUnit::CPUNanosecond:
        case CostUnit::QuerySlot:
            chassert(false); return 0;
        case CostUnit::MemoryByte: return max_memory;
    }
}


void WorkloadSettings::initFromChanges(const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name, bool throw_on_unknown_setting)
{
    struct {
        std::optional<Float64> weight;
        std::optional<Priority> priority;
        std::optional<Priority> precedence;
        std::optional<Float64> max_bytes_per_second;
        std::optional<Float64> max_burst_bytes;
        std::optional<Float64> max_cpus;
        std::optional<Float64> max_cpu_share;
        std::optional<Float64> max_burst_cpu_seconds;
        std::optional<Float64> max_queries_per_second;
        std::optional<Float64> max_burst_queries;
        std::optional<Int64> max_io_requests;
        std::optional<Int64> max_bytes_inflight;
        std::optional<Int64> max_concurrent_threads;
        std::optional<Float64> max_concurrent_threads_ratio_to_cores;
        std::optional<Int64> max_concurrent_queries;
        std::optional<Int64> max_waiting_queries;
        std::optional<Int64> max_memory;

        static Float64 getFloat64(const String & name, const Field & field)
        {
            // We dont mind slight loss of precision when converting from Int64/UInt64 to Float64
            {
                UInt64 val;
                if (field.tryGet(val))
                    return static_cast<Float64>(val);
            }

            {
                Int64 val;
                if (field.tryGet(val))
                    return static_cast<Float64>(val);
            }

            {
                String val; // To handle suffixes
                if (field.tryGet(val))
                    return static_cast<Float64>(parseWithSizeSuffix<Int64>(val));
            }

            Float64 value = field.safeGet<Float64>();
            if (!std::isfinite(value))
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER,
                    "Float setting value must be finite, got {} for workload setting '{}'", value, name);
            return value;
        }

        static Int64 getInt64(const Field & field)
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
                    return val;
            }

            {
                String val; // To handle suffixes
                if (field.tryGet(val))
                    return parseWithSizeSuffix<Int64>(val);
            }

            return field.safeGet<Int64>();
        }

        static Float64 getNotNegativeFloat64(const String & name, const Field & field)
        {
            Float64 val = getFloat64(name, field);
            if (val < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected negative value {} for workload setting '{}'", val, name);
            return val;
        }

        static Int64 getNotNegativeInt64(const String & name, const Field & field)
        {
            Int64 val = getInt64(field);
            if (val < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected negative value {} for workload setting '{}'", val, name);
            return val;
        }

        void read(const String & name, const Field & value, bool throw_on_unknown_setting)
        {
            // Note that the second workload setting name options are provided for backward-compatibility
            if (name == "weight")
                weight = getNotNegativeFloat64(name, value);
            else if (name == "priority")
                priority = Priority{value.safeGet<Priority::Value>()};
            else if (name == "precedence")
                precedence = Priority{value.safeGet<Priority::Value>()};
            else if (name == "max_bytes_per_second" || name == "max_speed")
                max_bytes_per_second = getNotNegativeFloat64(name, value);
            else if (name == "max_burst_bytes" || name == "max_burst")
                max_burst_bytes = getNotNegativeFloat64(name, value);
            else if (name == "max_cpus")
                max_cpus = getNotNegativeFloat64(name, value);
            else if (name == "max_cpu_share")
                max_cpu_share = getNotNegativeFloat64(name, value);
            else if (name == "max_burst_cpu_seconds")
                max_burst_cpu_seconds = getNotNegativeFloat64(name, value);
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
            else if (name == "max_waiting_queries")
                max_waiting_queries = getNotNegativeInt64(name, value);
            else if (name == "max_memory")
                max_memory = getNotNegativeInt64(name, value);
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
            get_value(specific.priority, regular.priority, priority),
            get_value(specific.precedence, regular.precedence, precedence));
    }

    // Choose values for given resource.
    // NOTE: previous values contain defaults.
    weight = get_value(specific.weight, regular.weight, weight);
    priority = get_value(specific.priority, regular.priority, priority);
    precedence = get_value(specific.precedence, regular.precedence, precedence);

    // IO throttling
    if (specific.max_bytes_per_second || regular.max_bytes_per_second)
    {
        max_bytes_per_second = get_value(specific.max_bytes_per_second, regular.max_bytes_per_second, max_bytes_per_second);
        // We always set max_burst_bytes if max_bytes_per_second is changed.
        // This is done for users to be able to ignore more advanced max_burst_bytes setting and rely only on max_bytes_per_second.
        max_burst_bytes = default_burst_seconds * max_bytes_per_second;
    }
    max_burst_bytes = get_value(specific.max_burst_bytes, regular.max_burst_bytes, max_burst_bytes);

    // CPU throttling
    if (specific.max_cpus || regular.max_cpus || specific.max_cpu_share || regular.max_cpu_share)
    {
        // Compute max_cpus as minimum of two possible values: (1) exact limit and (2) share limit.
        Float64 limit = 0;
        Float64 exact_limit = get_value(specific.max_cpus, regular.max_cpus, 0.0);
        Float64 share_limit = get_value(specific.max_cpu_share, regular.max_cpu_share, 0.0);
        if (exact_limit > 0)
            limit = exact_limit;
        if (share_limit > 0)
        {
            Float64 value = share_limit * getNumberOfCPUCoresToUse();
            if (value > 0 && (limit == 0 || value < limit))
                limit = value;
        }
        max_cpus = limit;
    }
    max_burst_cpu_seconds = get_value(specific.max_burst_cpu_seconds, regular.max_burst_cpu_seconds, max_burst_cpu_seconds);

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
    {
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
    }

    // Concurrent queries limit
    max_concurrent_queries = get_value(specific.max_concurrent_queries, regular.max_concurrent_queries, max_concurrent_queries, unlimited);

    // Concurrent query queue size limit
    max_waiting_queries = get_value(specific.max_waiting_queries, regular.max_waiting_queries, max_waiting_queries, unlimited);

    // Total memory reservation limit
    max_memory = get_value(specific.max_memory, regular.max_memory, max_memory, unlimited);
}

}
