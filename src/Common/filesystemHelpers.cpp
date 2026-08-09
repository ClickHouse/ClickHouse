#include <Common/filesystemHelpers.h>
#include <base/pathToString.h>
#include <limits>
#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#endif

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
#include <IO/PlatformFileIO.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
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
    struct statvfs fs{};
#if defined(OS_WINDOWS)
    while (true)
    {
        ULARGE_INTEGER available_to_caller{};
        ULARGE_INTEGER total{};
        ULARGE_INTEGER free_total{};
        if (GetDiskFreeSpaceExW(pathFromString(path).c_str(), &available_to_caller, &total, &free_total))
        {
            /// Report in bytes: `GetDiskFreeSpaceEx` gives byte counts, so a fragment is one byte
            /// and the free-space arithmetic elsewhere (`f_bavail * f_frsize`) comes out right.
            fs.f_frsize = 1;
            fs.f_blocks = total.QuadPart;
            fs.f_bavail = available_to_caller.QuadPart;
            /// NTFS has no fixed inode table, so there is no limit to report and no way to run
            /// out. `max` is the truthful answer here, where 0 would read as "no inodes left".
            fs.f_files = std::numeric_limits<unsigned long long>::max();
            fs.f_favail = std::numeric_limits<unsigned long long>::max();
            return fs;
        }

        /// Same as below: we sometimes ask about a directory that is yet to be created.
        auto fs_path = std::filesystem::path(path);
        const auto error = GetLastError();
        if ((error == ERROR_PATH_NOT_FOUND || error == ERROR_FILE_NOT_FOUND) && fs_path.has_parent_path())
        {
            path = pathToString(fs_path.parent_path());
            continue;
        }

        throw Exception(
            ErrorCodes::CANNOT_STATVFS,
            "Could not calculate available disk space for {} (GetDiskFreeSpaceEx), error code: {}",
            path,
            error);
    }
#else
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
#endif
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

std::unique_ptr<Poco::TemporaryFile> createTemporaryFile(const std::string & folder_path)
{
    ProfileEvents::increment(ProfileEvents::ExternalProcessingFilesTotal);
    fs::create_directories(folder_path);
    return std::make_unique<Poco::TemporaryFile>(folder_path);
}

#if !defined(OS_LINUX)
[[noreturn]]
#endif
String getBlockDeviceId([[maybe_unused]] const String & path)
{
#if defined(OS_LINUX)
    struct stat sb{};
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
    struct stat sb{};
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
        int rotational = 0;
        readText(rotational, in);
        return rotational ? BlockDeviceType::ROT : BlockDeviceType::NONROT;
    }
    catch (const std::exception &)
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
        int read_ahead_kb = 0;
        readText(read_ahead_kb, in);
        return read_ahead_kb * 1024;
    }
    catch (const std::exception &)
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
#if defined(OS_WINDOWS)
        /// `path::c_str()` is a `const wchar_t *` here, so the wide variant is the matching one -
        /// and the only one that can name a path outside the active code page.
        struct _stat64 st{};
        if (_wstat64(p.c_str(), &st))
            DB::ErrnoException::throwFromPath(DB::ErrorCodes::SYSTEM_ERROR, pathToString(p), "Cannot stat {}", pathToString(p));
        return st.st_dev;
#else
        struct stat st{};
        if (stat(p.c_str(), &st))   /// NOTE: man stat does not list EINTR as possible error
            DB::ErrnoException::throwFromPath(DB::ErrorCodes::SYSTEM_ERROR, p.string(), "Cannot stat {}", p.string());
        return st.st_dev;
#endif
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
    mntent fs_info{};
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
    auto rel = fs::relative(path, prefix_path);
    if (rel.empty() || rel == "..")
        return false;

    while (rel.has_relative_path())
    {
        rel = rel.parent_path();
        if (rel == "..")
            return false;
    }

    return true;
}

static bool fileOrSymlinkPathStartsWith(const std::filesystem::path & path, const std::filesystem::path & prefix_path)
{
    /// Differs from pathStartsWith in how `path` is normalized before comparison.
    /// Make `path` absolute if it was relative and put it into normalized form: remove
    /// `.` and `..` and extra `/`. Path is not canonized because otherwise path will
    /// not be a path of a symlink itself.

    auto rel = fs::absolute(path).lexically_normal().lexically_relative(fs::absolute(prefix_path).lexically_normal());

    if (rel.empty() || rel == "..")
        return false;

    while (rel.has_relative_path())
    {
        rel = rel.parent_path();
        if (rel == "..")
            return false;
    }

    return true;
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
    struct stat buf{};
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
    struct stat file_stat{};
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
    int n = DB::platformOpenFile(path, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (n != -1)
    {
        close(n);
        return true;
    }
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_CREATE_FILE, path, "Cannot create file: {}", path);
}

#if defined(OS_WINDOWS)
/// mingw-w64 does not define the `access` mode bits; the CRT's own spellings are these values.
#ifndef F_OK
#define F_OK 0
#endif
#ifndef W_OK
#define W_OK 2
#endif
#ifndef R_OK
#define R_OK 4
#endif
#ifndef X_OK
#define X_OK 1
#endif
#endif

/// `access(2)` against the current directory, which is what the `AT_FDCWD` below asks for.
///
/// Windows has no `faccessat`, and `_waccess` differs in two ways worth knowing. Its mode bits
/// happen to match F_OK/W_OK/R_OK above but it has no execute bit at all - Windows decides
/// whether a file can be executed from its contents, not from a permission - so an `X_OK` query
/// degrades to asking whether the file exists. And it reports only the read-only attribute and
/// ACL-based denial, not the calling user's effective permissions the way `AT_EACCESS` does.
static int checkAccess(const std::string & path, int mode)
{
#if defined(OS_WINDOWS)
    const int windows_mode = (mode == X_OK) ? F_OK : mode;
    return ::_waccess(pathFromString(path).c_str(), windows_mode);
#else
    return faccessat(AT_FDCWD, path.c_str(), mode, AT_EACCESS);
#endif
}

bool exists(const std::string & path)
{
    return checkAccess(path, F_OK) == 0;
}

bool canRead(const std::string & path, bool allow_throw)
{
    int err = checkAccess(path, R_OK);
    if (err == 0)
        return true;

    if (errno == EACCES)
        return false;

    if (!allow_throw)
        return false;

    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot check read access to file: {}", path);
}

bool canWrite(const std::string & path, bool allow_throw)
{
    int err = checkAccess(path, W_OK);
    if (err == 0)
        return true;

    if (errno == EACCES)
        return false;

    if (!allow_throw)
        return false;

    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot check write access to file: {}", path);
}

bool canExecute(const std::string & path, bool allow_throw)
{
    int err = checkAccess(path, X_OK);
    if (err == 0)
        return true;

    if (errno == EACCES)
        return false;

    if (!allow_throw)
        return false;

    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot check execute access to file: {}", path);
}

time_t getModificationTime(const std::string & path)
{
    struct stat st{};
    if (DB::platformStat(path, st) == 0)
        return st.st_mtime;
    std::error_code m_ec(errno, std::generic_category());
    throw fs::filesystem_error("Cannot check modification time for file", path, m_ec);
}

time_t getChangeTime(const std::string & path)
{
    struct stat st{};
    if (DB::platformStat(path, st) == 0)
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
    if (DB::platformSetFileTimes(path, time, time) != 0)
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
