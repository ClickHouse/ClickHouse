#pragma once

#include <atomic>
#include <memory>

#include <base/types.h>
#include <Common/MemoryTracker.h>


namespace DB
{

struct MemorySnapshot
{
    Int64 used_bytes = 0;
    Int64 hard_limit_bytes = 0;  /// 0 = unlimited
};

class IQueryMemoryProvider
{
public:
    virtual MemorySnapshot get() const = 0;
    virtual ~IQueryMemoryProvider() = default;
};

class QueryMemoryProvider : public IQueryMemoryProvider
{
public:
    explicit QueryMemoryProvider(MemoryTracker * tracker_) : tracker(tracker_) {}

    MemorySnapshot get() const override;

private:
    MemoryTracker * tracker = nullptr;
};


/// Memory-based throttle for INSERT pipelines
/// Estimates per-chunk cost from observed `Chunk::bytes()` (EWMA) and throttles
/// parallelism so that `safety_factor * avg_chunk_bytes * allowed_outputs` stays
/// below the free memory budget
class InsertMemoryThrottle
{
public:
    /// Multiplier applied to the average chunk size to account for downstream
    /// amplification not seen at the resize point
    static constexpr double DEFAULT_SAFETY_FACTOR = 4.0;

    /// Smoothing factor of the chunk-bytes EWMA: higher = react faster to new chunks,
    /// lower = smoother estimate
    static constexpr double EWMA_ALPHA = 0.25;

    explicit InsertMemoryThrottle(
        std::shared_ptr<IQueryMemoryProvider> mem_provider_,
        double safety_factor_ = DEFAULT_SAFETY_FACTOR)
        : mem_provider(std::move(mem_provider_))
        , safety_factor(safety_factor_)
    {}

    void observeChunkBytes(size_t bytes);

    size_t calculateAllowedOutputs(size_t max_outputs, size_t min_outputs = 1);

    Int64 getAverageChunkBytes() const { return avg_chunk_bytes.load(std::memory_order_relaxed); }

private:
    std::shared_ptr<IQueryMemoryProvider> mem_provider;
    double safety_factor;

    std::atomic<Int64> avg_chunk_bytes{0};
};

using InsertMemoryThrottlePtr = std::shared_ptr<InsertMemoryThrottle>;

}
