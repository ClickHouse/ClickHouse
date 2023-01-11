#include "VolumeJBOD.h"

#include <Common/StringUtils/StringUtils.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <base/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

VolumeJBOD::VolumeJBOD(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector)
    : IVolume(name_, config, config_prefix, disk_selector)
{
    Poco::Logger * logger = &Poco::Logger::get("StorageConfiguration");

    auto has_max_bytes = config.has(config_prefix + ".max_data_part_size_bytes");
    auto has_max_ratio = config.has(config_prefix + ".max_data_part_size_ratio");
    if (has_max_bytes && has_max_ratio)
        throw Exception(
            "Only one of 'max_data_part_size_bytes' and 'max_data_part_size_ratio' should be specified.",
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    if (has_max_bytes)
    {
        max_data_part_size = config.getUInt64(config_prefix + ".max_data_part_size_bytes", 0);
    }
    else if (has_max_ratio)
    {
        auto ratio = config.getDouble(config_prefix + ".max_data_part_size_ratio");
        if (ratio < 0)
            throw Exception("'max_data_part_size_ratio' have to be not less then 0.", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        UInt64 sum_size = 0;
        std::vector<UInt64> sizes;
        for (const auto & disk : disks)
        {
            sizes.push_back(disk->getTotalSpace());
            sum_size += sizes.back();
        }
        max_data_part_size = static_cast<decltype(max_data_part_size)>(sum_size * ratio / disks.size());
        for (size_t i = 0; i < disks.size(); ++i)
            if (sizes[i] < max_data_part_size)
                LOG_WARNING(logger, "Disk {} on volume {} have not enough space ({}) for containing part the size of max_data_part_size ({})", backQuote(disks[i]->getName()), backQuote(config_prefix), ReadableSize(sizes[i]), ReadableSize(max_data_part_size));
    }
    static constexpr UInt64 MIN_PART_SIZE = 8u * 1024u * 1024u;
    if (max_data_part_size != 0 && max_data_part_size < MIN_PART_SIZE)
        LOG_WARNING(logger, "Volume {} max_data_part_size is too low ({} < {})", backQuote(name), ReadableSize(max_data_part_size), ReadableSize(MIN_PART_SIZE));

    /// Default value is 'true' due to backward compatibility.
    perform_ttl_move_on_insert = config.getBool(config_prefix + ".perform_ttl_move_on_insert", true);

    are_merges_avoided = config.getBool(config_prefix + ".prefer_not_to_merge", false);
}

VolumeJBOD::VolumeJBOD(const VolumeJBOD & volume_jbod,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector)
    : VolumeJBOD(volume_jbod.name, config, config_prefix, disk_selector)
{
    are_merges_avoided_user_override = volume_jbod.are_merges_avoided_user_override.load(std::memory_order_relaxed);
    last_used = volume_jbod.last_used.load(std::memory_order_relaxed);
}

DiskPtr VolumeJBOD::getDisk(size_t /* index */) const
{
    size_t start_from = last_used.fetch_add(1u, std::memory_order_acq_rel);
    size_t index = start_from % disks.size();
    return disks[index];
}

ReservationPtr VolumeJBOD::reserve(UInt64 bytes)
{
    /// This volume can not store data which size is greater than `max_data_part_size`
    /// to ensure that parts of size greater than that go to another volume(s).

    if (max_data_part_size != 0 && bytes > max_data_part_size)
        return {};

    size_t start_from = last_used.fetch_add(1u, std::memory_order_acq_rel);
    size_t disks_num = disks.size();
    for (size_t i = 0; i < disks_num; ++i)
    {
        size_t index = (start_from + i) % disks_num;

        auto reservation = disks[index]->reserve(bytes);

        if (reservation)
            return reservation;
    }
    return {};
}

bool VolumeJBOD::areMergesAvoided() const
{
    auto are_merges_avoided_user_override_value = are_merges_avoided_user_override.load(std::memory_order_acquire);
    if (are_merges_avoided_user_override_value)
        return *are_merges_avoided_user_override_value;
    else
        return are_merges_avoided;
}

void VolumeJBOD::setAvoidMergesUserOverride(bool avoid)
{
    are_merges_avoided_user_override.store(avoid, std::memory_order_release);
}


}
