#include "VFSSettings.h"
#include "Common/Exception.h"

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

VFSNodes::VFSNodes(std::string_view disk_vfs_id)
    : base(fmt::format("/vfs/{}", disk_vfs_id))
    , log_base(base + "/ops")
    , log_item(log_base + "/log-")
    , gc_lock(base + "/gc_lock")
{
}

VFSSettings::VFSSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    double keeper_fault_injection_probability_,
    UInt64 keeper_fault_injection_seed_)
    : gc_sleep_ms(config.getUInt64(config_prefix + ".vfs_gc_sleep_ms", 10'000))
    , batch_min_size(config.getUInt64(config_prefix + ".vfs_batch_min_size", 1))
    , batch_max_size(config.getUInt64(config_prefix + ".vfs_batch_max_size", 10'000))
    , batch_can_wait_ms(config.getUInt64(config_prefix + ".vfs_batch_can_wait_ms", 0))
    , snapshot_lz4_compression_level(config.getInt(config_prefix + ".vfs_snapshot_lz4_compression_level", 8))
    , keeper_fault_injection_probability(keeper_fault_injection_probability_)
    , keeper_fault_injection_seed(keeper_fault_injection_seed_)
{
    if (gc_sleep_ms == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "gc_sleep_ms == 0");
    if (batch_min_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "batch_min_size == 0");
    if (batch_max_size <= batch_min_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "batch_max_size ({}) <= batch_min_size ({})", batch_max_size, batch_min_size);
}
}
