#include <Common/renameat2.h>
#include <Common/Exception.h>
#include <Poco/File.h>

#if defined(linux) || defined(__linux) || defined(__linux__)
#include <unistd.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <linux/fs.h>
#include <sys/utsname.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ATOMIC_RENAME_FAIL;
    extern const int SYSTEM_ERROR;
    extern const int UNSUPPORTED_METHOD;
    extern const int FILE_ALREADY_EXISTS;
}

static bool supportsRenameat2Impl()
{
#if defined(__NR_renameat2)
    /// renameat2 is available in linux since 3.15
    struct utsname sysinfo;
    if (uname(&sysinfo))
        return false;
    char * point = nullptr;
    auto v_major = strtol(sysinfo.release, &point, 10);

    errno = 0;
    if (errno || *point != '.' || v_major < 3)
        return false;
    if (3 < v_major)
        return true;

    errno = 0;
    auto v_minor = strtol(point + 1, nullptr, 10);
    return !errno && 15 <= v_minor;
#else
    return false;
#endif
}

#if defined(__NR_renameat2)

static bool renameat2(const std::string & old_path, const std::string & new_path, int flags)
{
    if (!supportsRenameat2())
        return false;
    if (old_path.empty() || new_path.empty())
        throw Exception("Cannot rename " + old_path + " to " + new_path + ": path is empty", ErrorCodes::LOGICAL_ERROR);

    /// int olddirfd (ignored for absolute oldpath), const char *oldpath,
    /// int newdirfd (ignored for absolute newpath), const char *newpath,
    /// unsigned int flags
    if (0 == syscall(__NR_renameat2, AT_FDCWD, old_path.c_str(), AT_FDCWD, new_path.c_str(), flags))
        return true;

    /// EINVAL means that filesystem does not support one of the flags.
    /// It also may happen when running clickhouse in docker with Mac OS as a host OS.
    /// supportsRenameat2() with uname is not enough in this case, because virtualized Linux kernel is used.
    /// Other cases when EINVAL can be returned should never happen.
    if (errno == EINVAL)
        return false;
    /// We should never get ENOSYS on Linux, because we check kernel version in supportsRenameat2Impl().
    /// However, we can get in on WSL.
    if (errno == ENOSYS)
        return false;

    if (errno == EEXIST)
        throwFromErrno("Cannot rename " + old_path + " to " + new_path + " because the second path already exists", ErrorCodes::ATOMIC_RENAME_FAIL);
    if (errno == ENOENT)
        throwFromErrno("Paths cannot be exchanged because " + old_path + " or " + new_path + " does not exist", ErrorCodes::ATOMIC_RENAME_FAIL);
    throwFromErrnoWithPath("Cannot rename " + old_path + " to " + new_path, new_path, ErrorCodes::SYSTEM_ERROR);
}

#else
#define RENAME_NOREPLACE -1
#define RENAME_EXCHANGE -1

static bool renameat2(const std::string &, const std::string &, int)
{
    return false;
}

#endif

static void renameNoReplaceFallback(const std::string & old_path, const std::string & new_path)
{
    /// NOTE it's unsafe
    if (Poco::File{new_path}.exists())
        throw Exception("File " + new_path + " exists", ErrorCodes::FILE_ALREADY_EXISTS);
    Poco::File{old_path}.renameTo(new_path);
}

/// Do not use [[noreturn]] to avoid warnings like "code will never be executed" in other places
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-noreturn"
static void renameExchangeFallback(const std::string &, const std::string &)
{
    throw Exception("System call renameat2() is not supported", ErrorCodes::UNSUPPORTED_METHOD);
}
#pragma GCC diagnostic pop


bool supportsRenameat2()
{
    static bool supports = supportsRenameat2Impl();
    return supports;
}

void renameNoReplace(const std::string & old_path, const std::string & new_path)
{
    if (!renameat2(old_path, new_path, RENAME_NOREPLACE))
        renameNoReplaceFallback(old_path, new_path);
}

void renameExchange(const std::string & old_path, const std::string & new_path)
{
    if (!renameat2(old_path, new_path, RENAME_EXCHANGE))
        renameExchangeFallback(old_path, new_path);
}

bool renameExchangeIfSupported(const std::string & old_path, const std::string & new_path)
{
    return renameat2(old_path, new_path, RENAME_EXCHANGE);
}

}
