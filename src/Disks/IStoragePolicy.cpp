#include <Disks/IStoragePolicy.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_VOLUME;
    extern const int UNKNOWN_DISK;
}

DiskPtr IStoragePolicy::getDiskByName(const String & disk_name) const
{
    auto disk = tryGetDiskByName(disk_name);
    if (!disk)
        throw Exception(ErrorCodes::UNKNOWN_DISK,
            "No such disk {} in storage policy {}", backQuote(disk_name), backQuote(getName()));

    return disk;
}

VolumePtr IStoragePolicy::getVolumeByName(const String & volume_name) const
{
    auto volume = tryGetVolumeByName(volume_name);
    if (!volume)
        throw Exception(ErrorCodes::UNKNOWN_VOLUME,
            "No such volume {} in storage policy {}", backQuote(volume_name), backQuote(getName()));

    return volume;
}

}
