#include <Common/filesystemHelpers.h>

#if defined(OS_LINUX)
#    include <mntent.h>
#    include <sys/sysmacros.h>
#endif
#include <cerrno>
#include <Poco/Timestamp.h>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Disks/IDisk.h>

namespace fs = std::filesystem;


namespace ProfileEvents
{
    extern const Event ExternalProcessingFilesTotal;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_STAT;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_STATVFS;
    extern const int PATH_ACCESS_DENIED;
    extern const int CANNOT_CREATE_FILE;
}

struct statvfs getStatVFS(String path)
{
    struct statvfs fs;
    while (statvfs(path.c_str(), &fs) != 0)
    {
        if (errno == EINTR)
            continue;

        /// Sometimes we create directories lazily, so we can request free space in a directory that yet to be created.
        auto fs_path = std::filesystem::path(path);
        if (errno == ENOENT && fs_path.has_parent_path())
        {
            path = fs_path.parent_path();
            continue;
        }

        ErrnoException::throwFromPath(ErrorCodes::CANNOT_STATVFS, path, "Could not calculate available disk space (statvfs)");
    }
    return fs;
}

bool enoughSpaceInDirectory(const std::string & path, size_t data_size)
{
    fs::path filepath(path);
    /// `path` may point to nonexisting file, then we can't check it directly, move to parent directory
    while (filepath.has_parent_path() && !fs::exists(filepath))
        filepath = filepath.parent_path();
    auto free_space = fs::space(filepath).free;
    return data_size <= free_space;
}

std::unique_ptr<PocoTemporaryFile> createTemporaryFile(const std::string & folder_path)
{
    ProfileEvents::increment(ProfileEvents::ExternalProcessingFilesTotal);
    fs::create_directories(folder_path);
    return std::make_unique<PocoTemporaryFile>(folder_path);
}

#if !defined(OS_LINUX)
[[noreturn]]
#endif
String getBlockDeviceId([[maybe_unused]] const String & path)
{
#if defined(OS_LINUX)
    struct stat sb;
    if (lstat(path.c_str(), &sb))
        DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_STAT, path, "Cannot lstat {}", path);
    WriteBufferFromOwnString ss;
    ss << major(sb.st_dev) << ":" << minor(sb.st_dev);
    return ss.str();
#else
    throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "The function getDeviceId is supported on Linux only");
#endif
}


std::optional<String> tryGetBlockDeviceId([[maybe_unused]] const String & path)
{
#if defined(OS_LINUX)
    struct stat sb;
    if (lstat(path.c_str(), &sb))
        return {};
    WriteBufferFromOwnString ss;
    ss << major(sb.st_dev) << ":" << minor(sb.st_dev);
    return ss.str();
#else
    return {};
#endif

}

#if !defined(OS_LINUX)
[[noreturn]]
#endif
BlockDeviceType getBlockDeviceType([[maybe_unused]] const String & device_id)
{
#if defined(OS_LINUX)
    try
    {
        const auto path{std::filesystem::path("/sys/dev/block/") / device_id / "queue/rotational"};
        if (!std::filesystem::exists(path))
            return BlockDeviceType::UNKNOWN;
        ReadBufferFromFile in(path);
        int rotational;
        readText(rotational, in);
        return rotational ? BlockDeviceType::ROT : BlockDeviceType::NONROT;
    }
    catch (...)
    {
        return BlockDeviceType::UNKNOWN;
    }
#else
    throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "The function getDeviceType is supported on Linux only");
#endif
}

#if !defined(OS_LINUX)
[[noreturn]]
#endif
UInt64 getBlockDeviceReadAheadBytes([[maybe_unused]] const String & device_id)
{
#if defined(OS_LINUX)
    try
    {
        const auto path{std::filesystem::path("/sys/dev/block/") / device_id / "queue/read_ahead_kb"};
        ReadBufferFromFile in(path);
        int read_ahead_kb;
        readText(read_ahead_kb, in);
        return read_ahead_kb * 1024;
    }
    catch (...)
    {
        return static_cast<UInt64>(-1);
    }
#else
    throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "The function getDeviceType is supported on Linux only");
#endif
}

/// Returns name of filesystem mounted to mount_point
std::filesystem::path getMountPoint(std::filesystem::path absolute_path)
{
    if (absolute_path.is_relative())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path is relative. It's a bug.");

    absolute_path = std::filesystem::canonical(absolute_path);

    const auto get_device_id = [](const std::filesystem::path & p)
    {
        struct stat st;
        if (stat(p.c_str(), &st))   /// NOTE: man stat does not list EINTR as possible error
            DB::ErrnoException::throwFromPath(DB::ErrorCodes::SYSTEM_ERROR, p.string(), "Cannot stat {}", p.string());
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
#if !defined(OS_LINUX)
[[noreturn]]
#endif
String getFilesystemName([[maybe_unused]] const String & mount_point)
{
#if defined(OS_LINUX)
    FILE * mounted_filesystems = setmntent("/etc/mtab", "r");
    if (!mounted_filesystems)
        throw DB::Exception(ErrorCodes::SYSTEM_ERROR, "Cannot open /etc/mtab to get name of filesystem");
    mntent fs_info;
    constexpr size_t buf_size = 4096;     /// The same as buffer used for getmntent in glibc. It can happen that it's not enough
    std::vector<char> buf(buf_size);
    while (getmntent_r(mounted_filesystems, &fs_info, buf.data(), buf_size) && fs_info.mnt_dir != mount_point)
        ;
    endmntent(mounted_filesystems);
    if (fs_info.mnt_dir != mount_point)
        throw DB::Exception(ErrorCodes::SYSTEM_ERROR, "Cannot find name of filesystem by mount point {}", mount_point);
    return fs_info.mnt_fsname;
#else
    throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "The function getFilesystemName is supported on Linux only");
#endif
}

bool pathStartsWith(const std::filesystem::path & path, const std::filesystem::path & prefix_path)
{
    String absolute_path = std::filesystem::weakly_canonical(path);
    String absolute_prefix_path = std::filesystem::weakly_canonical(prefix_path);
    return absolute_path.starts_with(absolute_prefix_path);
}

static bool fileOrSymlinkPathStartsWith(const std::filesystem::path & path, const std::filesystem::path & prefix_path)
{
    /// Differs from pathStartsWith in how `path` is normalized before comparison.
    /// Make `path` absolute if it was relative and put it into normalized form: remove
    /// `.` and `..` and extra `/`. Path is not canonized because otherwise path will
    /// not be a path of a symlink itself.

    String absolute_path = std::filesystem::absolute(path);
    absolute_path = fs::path(absolute_path).lexically_normal(); /// Normalize path.
    String absolute_prefix_path = std::filesystem::absolute(prefix_path);
    absolute_prefix_path = fs::path(absolute_prefix_path).lexically_normal(); /// Normalize path.
    return absolute_path.starts_with(absolute_prefix_path);
}

bool pathStartsWith(const String & path, const String & prefix_path)
{
    auto filesystem_path = std::filesystem::path(path);
    auto filesystem_prefix_path = std::filesystem::path(prefix_path);

    return pathStartsWith(filesystem_path, filesystem_prefix_path);
}

bool fileOrSymlinkPathStartsWith(const String & path, const String & prefix_path)
{
    auto filesystem_path = std::filesystem::path(path);
    auto filesystem_prefix_path = std::filesystem::path(prefix_path);

    return fileOrSymlinkPathStartsWith(filesystem_path, filesystem_prefix_path);
}

size_t getSizeFromFileDescriptor(int fd, const String & file_name)
{
    struct stat buf;
    int res = fstat(fd, &buf);
    if (-1 == res)
    {
        DB::ErrnoException::throwFromPath(
            DB::ErrorCodes::CANNOT_FSTAT, file_name, "Cannot execute fstat{}", file_name.empty() ? "" : " file: " + file_name);
    }
    return buf.st_size;
}

Int64 getINodeNumberFromPath(const String & path)
{
    struct stat file_stat;
    if (stat(path.data(), &file_stat))
    {
        DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_STAT, path, "Cannot execute stat for file {}", path);
    }
    return file_stat.st_ino;
}

std::optional<size_t> tryGetSizeFromFilePath(const String & path)
{
    std::error_code ec;

    size_t size = fs::file_size(path, ec);
    if (!ec)
        return size;

    if (ec == std::errc::no_such_file_or_directory)
        return std::nullopt;
    if (ec == std::errc::operation_not_supported)
        return std::nullopt;

    throw fs::filesystem_error("Got unexpected error while getting file size", path, ec);
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
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_CREATE_FILE, path, "Cannot create file: {}", path);
}

bool exists(const std::string & path)
{
    return faccessat(AT_FDCWD, path.c_str(), F_OK, AT_EACCESS) == 0;
}

bool canRead(const std::string & path)
{
    int err = faccessat(AT_FDCWD, path.c_str(), R_OK, AT_EACCESS);
    if (err == 0)
        return true;
    if (errno == EACCES)
        return false;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot check read access to file: {}", path);
}

bool canWrite(const std::string & path)
{
    int err = faccessat(AT_FDCWD, path.c_str(), W_OK, AT_EACCESS);
    if (err == 0)
        return true;
    if (errno == EACCES)
        return false;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot check write access to file: {}", path);
}

bool canExecute(const std::string & path)
{
    int err = faccessat(AT_FDCWD, path.c_str(), X_OK, AT_EACCESS);
    if (err == 0)
        return true;
    if (errno == EACCES)
        return false;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot check execute access to file: {}", path);
}

time_t getModificationTime(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
        return st.st_mtime;
    std::error_code m_ec(errno, std::generic_category());
    throw fs::filesystem_error("Cannot check modification time for file", path, m_ec);
}

time_t getChangeTime(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
        return st.st_ctime;
    std::error_code m_ec(errno, std::generic_category());
    throw fs::filesystem_error("Cannot check change time for file", path, m_ec);
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
        DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot set modification time to file: {}", path);
}

bool isSymlink(const fs::path & path)
{
    /// Remove trailing slash before checking if file is symlink.
    /// Let /path/to/link is a symlink to /path/to/target/dir/ directory.
    /// In this case is_symlink("/path/to/link") is true,
    /// but is_symlink("/path/to/link/") is false (it's a directory)
    if (path.filename().empty())
        return fs::is_symlink(path.parent_path());      /// STYLE_CHECK_ALLOW_STD_FS_SYMLINK
    return fs::is_symlink(path);        /// STYLE_CHECK_ALLOW_STD_FS_SYMLINK
}

bool isSymlinkNoThrow(const fs::path & path)
{
    std::error_code dummy;
    if (path.filename().empty())
        return fs::is_symlink(path.parent_path(), dummy);      /// STYLE_CHECK_ALLOW_STD_FS_SYMLINK
    return fs::is_symlink(path, dummy);        /// STYLE_CHECK_ALLOW_STD_FS_SYMLINK
}

fs::path readSymlink(const fs::path & path)
{
    /// See the comment for isSymlink
    if (path.filename().empty())
        return fs::read_symlink(path.parent_path());        /// STYLE_CHECK_ALLOW_STD_FS_SYMLINK
    return fs::read_symlink(path);      /// STYLE_CHECK_ALLOW_STD_FS_SYMLINK
}

}
