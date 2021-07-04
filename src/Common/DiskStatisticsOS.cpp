#if defined(OS_LINUX)

#include <Common/DiskStatisticsOS.h>
#include <Common/filesystemHelpers.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_STATVFS;
}


DiskStatisticsOS::Data DiskStatisticsOS::get()
{
    ReadBufferFromFile mounts_in("/proc/mounts", 4096 /* arbitrary small buffer */);

    Data data{};

    std::string fs_device;
    std::string fs_path;

    while (!mounts_in.eof())
    {
        readStringUntilWhitespace(fs_device, mounts_in);
        skipWhitespaceIfAny(mounts_in);
        readStringUntilWhitespace(fs_path, mounts_in);
        skipWhitespaceIfAny(mounts_in);

        /// Only real devices
        if (!fs_device.starts_with("/dev/") || fs_device.starts_with("/dev/loop"))
            continue;

        struct statvfs stat = getStatVFS(fs_path);

        data.total_bytes += (stat.f_blocks) * stat.f_bsize;
        data.used_bytes += (stat.f_blocks - stat.f_bfree) * stat.f_bsize;
        data.total_inodes += stat.f_files;
        data.used_inodes += stat.f_files - stat.f_ffree;
    }

    return data;
}

}

#endif
