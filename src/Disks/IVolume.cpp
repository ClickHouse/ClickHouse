#include "IVolume.h"

#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INCONSISTENT_RESERVATIONS;
    extern const int NO_RESERVATIONS_PROVIDED;
    extern const int UNKNOWN_VOLUME_TYPE;
}

String volumeTypeToString(VolumeType type)
{
    switch (type)
    {
        case VolumeType::JBOD:
            return "JBOD";
        case VolumeType::RAID1:
            return "RAID1";
        case VolumeType::SINGLE_DISK:
            return "SINGLE_DISK";
        case VolumeType::UNKNOWN:
            return "UNKNOWN";
    }
    throw Exception("Unknown volume type, please add it to DB::volumeTypeToString", ErrorCodes::UNKNOWN_VOLUME_TYPE);
}

IVolume::IVolume(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector)
    : name(std::move(name_))
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & disk : keys)
    {
        if (startsWith(disk, "disk"))
        {
            auto disk_name = config.getString(config_prefix + "." + disk);
            disks.push_back(disk_selector->get(disk_name));
        }
    }

    if (disks.empty())
        throw Exception("Volume must contain at least one disk.", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
}

UInt64 IVolume::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & disk : disks)
        res = std::max(res, disk->getUnreservedSpace());
    return res;
}

MultiDiskReservation::MultiDiskReservation(Reservations & reservations_, UInt64 size_)
    : reservations(std::move(reservations_))
    , size(size_)
{
    if (reservations.empty())
    {
        throw Exception("At least one reservation must be provided to MultiDiskReservation", ErrorCodes::NO_RESERVATIONS_PROVIDED);
    }

    for (auto & reservation : reservations)
    {
        if (reservation->getSize() != size_)
        {
            throw Exception("Reservations must have same size", ErrorCodes::INCONSISTENT_RESERVATIONS);
        }
    }
}

Disks MultiDiskReservation::getDisks() const
{
    Disks res;
    res.reserve(reservations.size());
    for (const auto & reservation : reservations)
    {
        res.push_back(reservation->getDisk());
    }
    return res;
}

void MultiDiskReservation::update(UInt64 new_size)
{
    for (auto & reservation : reservations)
    {
        reservation->update(new_size);
    }
    size = new_size;
}


}
