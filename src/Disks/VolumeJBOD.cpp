#include <Disks/VolumeJBOD.h>

#include <Common/StringUtils.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>

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
    , disks_by_size(disks.begin(), disks.end())
{
    LoggerPtr logger = getLogger("StorageConfiguration");

    volume_priority = config.getUInt64(config_prefix + ".volume_priority", std::numeric_limits<UInt64>::max());

    auto has_max_bytes = config.has(config_prefix + ".max_data_part_size_bytes");
    auto has_max_ratio = config.has(config_prefix + ".max_data_part_size_ratio");
    if (has_max_bytes && has_max_ratio)
    {
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                        "Only one of 'max_data_part_size_bytes' and 'max_data_part_size_ratio' should be specified.");
    }

    if (has_max_bytes)
    {
        max_data_part_size = config.getUInt64(config_prefix + ".max_data_part_size_bytes", 0);
    }
    else if (has_max_ratio)
    {
        auto ratio = config.getDouble(config_prefix + ".max_data_part_size_ratio");
        if (ratio < 0)
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "'max_data_part_size_ratio' have to be not less then 0.");

        UInt64 sum_size = 0;
        std::vector<UInt64> sizes;
        for (const auto & disk : disks)
        {
            auto size = disk->getTotalSpace();
            if (size)
                sum_size += *size;
            else
                break;
            sizes.push_back(*size);
        }
        if (sizes.size() == disks.size())
        {
            max_data_part_size = static_cast<UInt64>(sum_size * ratio / disks.size());
            for (size_t i = 0; i < disks.size(); ++i)
            {
                if (sizes[i] < max_data_part_size)
                {
                    LOG_WARNING(logger, "Disk {} on volume {} have not enough space ({}) for containing part the size of max_data_part_size ({})",
                        backQuote(disks[i]->getName()), backQuote(config_prefix), ReadableSize(sizes[i]), ReadableSize(max_data_part_size));
                }
            }
        }
    }
    static constexpr UInt64 MIN_PART_SIZE = 8u * 1024u * 1024u;
    if (max_data_part_size != 0 && max_data_part_size < MIN_PART_SIZE)
    {
        LOG_WARNING(logger, "Volume {} max_data_part_size is too low ({} < {})",
            backQuote(name), ReadableSize(max_data_part_size), ReadableSize(MIN_PART_SIZE));
    }

    /// Default value is 'true' due to backward compatibility.
    perform_ttl_move_on_insert = config.getBool(config_prefix + ".perform_ttl_move_on_insert", true);

    are_merges_avoided = config.getBool(config_prefix + ".prefer_not_to_merge", false);
    least_used_ttl_ms = config.getUInt64(config_prefix + ".least_used_ttl_ms", 60'000);
}

VolumeJBOD::VolumeJBOD(const VolumeJBOD & volume_jbod,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector)
    : VolumeJBOD(volume_jbod.name, config, config_prefix, disk_selector)
{
    are_merges_avoided_user_override = volume_jbod.are_merges_avoided_user_override.load();
    last_used = volume_jbod.last_used.load(std::memory_order_relaxed);
}

DiskPtr VolumeJBOD::getDisk(size_t /* index */) const
{
    switch (load_balancing)
    {
        case VolumeLoadBalancing::ROUND_ROBIN:
        {
            size_t start_from = last_used.fetch_add(1u, std::memory_order_acq_rel);
            size_t index = start_from % disks.size();
            return disks[index];
        }
        case VolumeLoadBalancing::LEAST_USED:
        {
            std::lock_guard lock(mutex);
            if (!least_used_ttl_ms || least_used_update_watch.elapsedMilliseconds() >= least_used_ttl_ms)
            {
                disks_by_size = LeastUsedDisksQueue(disks.begin(), disks.end());
                least_used_update_watch.restart();
            }
            return disks_by_size.top().disk;
        }
    }
}

ReservationPtr VolumeJBOD::reserve(UInt64 bytes)
{
    /// This volume can not store data which size is greater than `max_data_part_size`
    /// to ensure that parts of size greater than that go to another volume(s).
    if (max_data_part_size != 0 && bytes > max_data_part_size)
        return {};

    switch (load_balancing)
    {
        case VolumeLoadBalancing::ROUND_ROBIN:
        {
            size_t disks_num = disks.size();
            for (size_t i = 0; i < disks_num; ++i)
            {
                size_t start_from = last_used.fetch_add(1u, std::memory_order_acq_rel);
                size_t index = start_from % disks_num;

                if (disks[index]->isReadOnly())
                    continue;

                auto reservation = disks[index]->reserve(bytes);

                if (reservation)
                    return reservation;
            }
            return {};
        }
        case VolumeLoadBalancing::LEAST_USED:
        {
            std::lock_guard lock(mutex);

            ReservationPtr reservation;
            for (size_t i = 0; i < disks.size() && !reservation; ++i)
            {
                if (i == 0 && (!least_used_ttl_ms || least_used_update_watch.elapsedMilliseconds() >= least_used_ttl_ms))
                {
                    disks_by_size = LeastUsedDisksQueue(disks.begin(), disks.end());
                    least_used_update_watch.restart();

                    DiskWithSize disk = disks_by_size.top();
                    if (!disk.disk->isReadOnly())
                        reservation = disk.reserve(bytes);
                }
                else
                {
                    DiskWithSize disk = disks_by_size.top();
                    disks_by_size.pop();

                    if (!disk.disk->isReadOnly())
                        reservation = disk.reserve(bytes);
                    disks_by_size.push(disk);
                }
            }

            return reservation;
        }
    }
}

bool VolumeJBOD::areMergesAvoided() const
{
    auto are_merges_avoided_user_override_value = are_merges_avoided_user_override.load(std::memory_order_acquire);
    if (are_merges_avoided_user_override_value)
        return *are_merges_avoided_user_override_value;
    return are_merges_avoided;
}

void VolumeJBOD::setAvoidMergesUserOverride(bool avoid)
{
    are_merges_avoided_user_override.store(avoid, std::memory_order_release);
}


}
