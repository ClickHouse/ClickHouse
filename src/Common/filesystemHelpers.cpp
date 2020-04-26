#include "filesystemHelpers.h"

#include <sys/stat.h>
#if defined(__linux__)
#    include <cstdio>
#    include <mntent.h>
#endif
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Version.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
    extern const int NOT_IMPLEMENTED;
}

bool enoughSpaceInDirectory(const std::string & path [[maybe_unused]], size_t data_size [[maybe_unused]])
{
#if POCO_VERSION >= 0x01090000
    auto free_space = Poco::File(path).freeSpace();
    return data_size <= free_space;
#else
    return true;
#endif
}

std::unique_ptr<TemporaryFile> createTemporaryFile(const std::string & path)
{
    Poco::File(path).createDirectories();

    /// NOTE: std::make_shared cannot use protected constructors
    return std::make_unique<TemporaryFile>(path);
}

std::filesystem::path getMountPoint(std::filesystem::path absolute_path)
{
    if (absolute_path.is_relative())
        throw Exception("Path is relative. It's a bug.", ErrorCodes::LOGICAL_ERROR);

    absolute_path = std::filesystem::canonical(absolute_path);

    const auto get_device_id = [](const std::filesystem::path & p)
    {
        struct stat st;
        if (stat(p.c_str(), &st))
            throwFromErrnoWithPath("Cannot stat " + p.string(), p.string(), ErrorCodes::SYSTEM_ERROR);
        return st.st_dev;
    };

    /// If /some/path/to/dir/ and /some/path/to/ have different device id,
    /// then device which contains /some/path/to/dir/filename is mounted to /some/path/to/dir/
    auto device_id = get_device_id(absolute_path);
    while (absolute_path.has_relative_path())
    {
        auto parent = absolute_path.parent_path();
        auto parent_device_id = get_device_id(parent);
        if (device_id != parent_device_id)
            return absolute_path;
        absolute_path = parent;
        device_id = parent_device_id;
    }

    return absolute_path;
}

/// Returns name of filesystem mounted to mount_point
#if !defined(__linux__)
[[noreturn]]
#endif
String getFilesystemName([[maybe_unused]] const String & mount_point)
{
#if defined(__linux__)
    FILE * mounted_filesystems = setmntent("/etc/mtab", "r");
    if (!mounted_filesystems)
        throw DB::Exception("Cannot open /etc/mtab to get name of filesystem", ErrorCodes::SYSTEM_ERROR);
    mntent fs_info;
    constexpr size_t buf_size = 4096;     /// The same as buffer used for getmntent in glibc. It can happen that it's not enough
    char buf[buf_size];
    while (getmntent_r(mounted_filesystems, &fs_info, buf, buf_size) && fs_info.mnt_dir != mount_point)
        ;
    endmntent(mounted_filesystems);
    if (fs_info.mnt_dir != mount_point)
        throw DB::Exception("Cannot find name of filesystem by mount point " + mount_point, ErrorCodes::SYSTEM_ERROR);
    return fs_info.mnt_fsname;
#else
    throw DB::Exception("The function getFilesystemName is supported on Linux only", ErrorCodes::NOT_IMPLEMENTED);
#endif
}

}
