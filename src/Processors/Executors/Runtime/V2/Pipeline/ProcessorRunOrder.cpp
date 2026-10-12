#include <Processors/Executors/Runtime/V2/Pipeline/ProcessorRunOrder.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace Runtime::V2
{

namespace
{

constexpr UInt64 light_work_ns = 5'000;

bool works(const ExecutingProcessor & record)
{
    const auto status = record.status.load();
    return status == ExecutingProcessor::Status::Work || status == ExecutingProcessor::Status::AsyncReady;
}

}

bool isLightWork(const ExecutingProcessor & record)
{
    return works(record) && record.work_ns > 0 && record.work_ns < light_work_ns;
}

bool allLightWork(std::span<ExecutingProcessor * const> records)
{
    return std::ranges::all_of(records, [](const ExecutingProcessor * record) { return isLightWork(*record); });
}

void sortRunnable(VectorWithMemoryTracking<ExecutingProcessor *> & runnable, const ExecutingProcessor & picked)
{
    UInt64 heaviest_parallel = 0;
    for (const auto * record : runnable)
        if (works(*record) && record->siblings > 1 && !isLightWork(*record))
            heaviest_parallel = std::max(heaviest_parallel, record->work_ns);

    const auto cost = [&](const ExecutingProcessor * record)
    {
        if (!works(*record))
            return UInt64(0);
        if (record->siblings > 1)
            return isLightWork(*record) ? UInt64(0) : record->work_ns;
        if (record == &picked && record->work_ns < heaviest_parallel)
            return std::numeric_limits<UInt64>::max();
        return UInt64(0);
    };

    std::stable_sort(runnable.begin(), runnable.end(), [&](const ExecutingProcessor * left, const ExecutingProcessor * right) { return cost(left) > cost(right); });
}

}

}
