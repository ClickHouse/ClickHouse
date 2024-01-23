#pragma once
#include <base/defines.h>
#include <fmt/core.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
struct VFSSettings
{
    // @c disk_name TODO myrrc implies disk name being consistent across replicas. Replace by vfs_disk_id
    VFSSettings(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        std::string_view disk_name,
        double keeper_fault_injection_probability_ = 0,
        UInt64 keeper_fault_injection_seed_ = 0)
        : gc_sleep_ms(config.getUInt64(config_prefix + ".vfs_gc_sleep_ms", 10'000))
        , batch_min_size(config.getUInt64(config_prefix + ".vfs_batch_min_size", 1))
        , batch_max_size(config.getUInt64(config_prefix + ".vfs_batch_max_size", 100'000))
        , batch_can_wait_ms(config.getUInt64(config_prefix + ".vfs_batch_can_wait_ms", 0))
        , snapshot_lz4_compression_level(config.getInt(config_prefix + ".vfs_snapshot_lz4_compression_level", 8))
        , base_node(fmt::format("/vfs_log/{}", disk_name))
        , locks_node(base_node + "/locks")
        , log_base_node(base_node + "/ops")
        , log_item(log_base_node + "/log-")
        , gc_lock_path(locks_node + "/gc_lock")
        , keeper_fault_injection_probability(keeper_fault_injection_probability_)
        , keeper_fault_injection_seed(keeper_fault_injection_seed_)
    {
    }

    size_t gc_sleep_ms;
    size_t batch_min_size;
    size_t batch_max_size;
    size_t batch_can_wait_ms;
    Int8 snapshot_lz4_compression_level;

    String base_node;
    String locks_node;
    String log_base_node;
    String log_item;
    String gc_lock_path;

    double keeper_fault_injection_probability{0};
    UInt64 keeper_fault_injection_seed{0};
};
}

template <>
struct fmt::formatter<DB::VFSSettings>
{
    constexpr auto parse(const auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::VFSSettings & obj, auto & ctx)
    {
        return fmt::format_to(
            ctx.out(),
            "gc_sleep_ms: {}\n"
            "batch_min_size: {}\n"
            "batch_max_size: {}\n"
            "batch_can_wait_ms: {}\n"
            "snapshot_lz4_compression_level: {}\n"
            "base_node: {}\n"
            "keeper_fault_injection_probability: {}\n"
            "keeper_fault_injection_seed: {}",
            obj.gc_sleep_ms,
            obj.batch_min_size,
            obj.batch_max_size,
            obj.batch_can_wait_ms,
            obj.snapshot_lz4_compression_level,
            obj.base_node,
            obj.keeper_fault_injection_probability,
            obj.keeper_fault_injection_seed);
    }
};
