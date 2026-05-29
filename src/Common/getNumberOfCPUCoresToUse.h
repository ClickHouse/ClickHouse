#pragma once

#include <cstddef>

/// Get the number of CPU cores to use. Depending on the machine size we choose
/// between the number of physical and logical cores.
/// Also under cgroups we respect possible cgroups limits.
unsigned getNumberOfCPUCoresToUse();

/// Resolve a thread pool size: if configured_size > 0, use it as-is.
/// If configured_size == 0 (auto), compute max(min_threads, cores * cores_ratio)
/// using getNumberOfCPUCoresToUse().
size_t resolveAutoPoolSize(size_t configured_size, size_t min_threads, double cores_ratio);

/// Default auto-sizing parameters for each background thread pool.
/// Used in Context.cpp (pool creation) and Server.cpp (config reload).
struct PoolAutoConfig
{
    size_t min_threads;
    double cores_ratio;
};

/// background_pool_size (merges and mutations)
static constexpr PoolAutoConfig background_pool_auto{16, 0.5};
/// background_move_pool_size
static constexpr PoolAutoConfig background_move_pool_auto{8, 0.25};
/// background_fetches_pool_size
static constexpr PoolAutoConfig background_fetches_pool_auto{16, 0.5};
/// background_common_pool_size
static constexpr PoolAutoConfig background_common_pool_auto{8, 0.25};
/// background_buffer_flush_schedule_pool_size
static constexpr PoolAutoConfig background_buffer_flush_pool_auto{16, 0.5};
/// background_schedule_pool_size
static constexpr PoolAutoConfig background_schedule_pool_auto{128, 4.0};
/// background_message_broker_schedule_pool_size
static constexpr PoolAutoConfig background_message_broker_pool_auto{16, 0.5};
/// background_distributed_schedule_pool_size
static constexpr PoolAutoConfig background_distributed_pool_auto{16, 0.5};
