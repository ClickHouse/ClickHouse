#include "filesystemHelpers.h"

#include <sys/stat.h>
#if defined(__linux__)
#    include <cstdio>
#    include <mntent.h>
#endif
#include <cerrno>
#include <Poco/Version.h>
#include <Poco/Timestamp.h>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <utime.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_STATVFS;
    extern const int PATH_ACCESS_DENIED;
    extern const int CANNOT_CREATE_FILE;
}


struct statvfs getStatVFS(const String & path)
{
    struct statvfs fs;
    while (statvfs(path.c_str(), &fs) != 0)
    {
        if (errno == EINTR)
            continue;
        throwFromErrnoWithPath("Could not calculate available disk space (statvfs)", path, ErrorCodes::CANNOT_STATVFS);
    }
    return fs;
}


bool enoughSpaceInDirectory(const std::string & path [[maybe_unused]], size_t data_size [[maybe_unused]])
{
    auto free_space = fs::space(path).free;
    return data_size <= free_space;
}

std::unique_ptr<TemporaryFile> createTemporaryFile(const std::string & path)
{
    fs::create_directories(path);

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
        if (stat(p.c_str(), &st))   /// NOTE: man stat does not list EINTR as possible error
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
    std::vector<char> buf(buf_size);
    while (getmntent_r(mounted_filesystems, &fs_info, buf.data(), buf_size) && fs_info.mnt_dir != mount_point)
        ;
    endmntent(mounted_filesystems);
    if (fs_info.mnt_dir != mount_point)
        throw DB::Exception("Cannot find name of filesystem by mount point " + mount_point, ErrorCodes::SYSTEM_ERROR);
    return fs_info.mnt_fsname;
#else
    throw DB::Exception("The function getFilesystemName is supported on Linux only", ErrorCodes::NOT_IMPLEMENTED);
#endif
}

bool pathStartsWith(const std::filesystem::path & path, const std::filesystem::path & prefix_path)
{
    auto absolute_path = std::filesystem::weakly_canonical(path);
    auto absolute_prefix_path = std::filesystem::weakly_canonical(prefix_path);

    auto [_, prefix_path_mismatch_it] = std::mismatch(absolute_path.begin(), absolute_path.end(), absolute_prefix_path.begin(), absolute_prefix_path.end());

    bool path_starts_with_prefix_path = (prefix_path_mismatch_it == absolute_prefix_path.end());
    return path_starts_with_prefix_path;
}

bool pathStartsWith(const String & path, const String & prefix_path)
{
    auto filesystem_path = std::filesystem::path(path);
    auto filesystem_prefix_path = std::filesystem::path(prefix_path);

    return pathStartsWith(filesystem_path, filesystem_prefix_path);
}

}


/// Copied from Poco::File
namespace FS
{

bool createFile(const std::string & path)
{
    int n = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (n != -1)
    {
        close(n);
        return true;
    }
    DB::throwFromErrnoWithPath("Cannot create file: " + path, path, DB::ErrorCodes::CANNOT_CREATE_FILE);
}

bool canRead(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
    {
        if (st.st_uid == geteuid())
            return (st.st_mode & S_IRUSR) != 0;
        else if (st.st_gid == getegid())
            return (st.st_mode & S_IRGRP) != 0;
        else
            return (st.st_mode & S_IROTH) != 0 || geteuid() == 0;
    }
    DB::throwFromErrnoWithPath("Cannot check read access to file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}


bool canWrite(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
    {
        if (st.st_uid == geteuid())
            return (st.st_mode & S_IWUSR) != 0;
        else if (st.st_gid == getegid())
            return (st.st_mode & S_IWGRP) != 0;
        else
            return (st.st_mode & S_IWOTH) != 0 || geteuid() == 0;
    }
    DB::throwFromErrnoWithPath("Cannot check write access to file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}

time_t getModificationTime(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
        return st.st_mtime;
    DB::throwFromErrnoWithPath("Cannot check modification time for file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}

Poco::Timestamp getModificationTimestamp(const std::string & path)
{
    return Poco::Timestamp::fromEpochTime(getModificationTime(path));
}

void setModificationTime(const std::string & path, time_t time)
{
    struct utimbuf tb;
    tb.actime  = time;
    tb.modtime = time;
    if (utime(path.c_str(), &tb) != 0)
        DB::throwFromErrnoWithPath("Cannot set modification time for file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}
}
