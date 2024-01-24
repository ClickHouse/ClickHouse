#pragma once
#include <base/defines.h>
#include <fmt/core.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
struct VFSTraits
{
    explicit VFSTraits(std::string_view disk_vfs_id);

    String base_node;
    String locks_node;
    String log_base_node;
    String log_item;
    String gc_lock_path;
};

struct VFSSettings
{
    VFSSettings(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        double keeper_fault_injection_probability_ = 0,
        UInt64 keeper_fault_injection_seed_ = 0);

    size_t gc_sleep_ms;
    size_t batch_min_size;
    size_t batch_max_size;
    size_t batch_can_wait_ms;
    int snapshot_lz4_compression_level;

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
            "keeper_fault_injection_probability: {}\n"
            "keeper_fault_injection_seed: {}",
            obj.gc_sleep_ms,
            obj.batch_min_size,
            obj.batch_max_size,
            obj.batch_can_wait_ms,
            obj.snapshot_lz4_compression_level,
            obj.keeper_fault_injection_probability,
            obj.keeper_fault_injection_seed);
    }
};
