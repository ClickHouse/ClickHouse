#include <Common/renameat2.h>
#include <Common/Exception.h>
#include <Common/VersionNumber.h>
#include <Poco/Environment.h>
#include <filesystem>

namespace fs = std::filesystem;

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

}


#if defined(__linux__)

#include <unistd.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <linux/fs.h>

/// For old versions of libc.
#if !defined(RENAME_NOREPLACE)
    #define RENAME_NOREPLACE 1
#endif

#if !defined(RENAME_EXCHANGE)
    #define RENAME_EXCHANGE 2
#endif

#if !defined(__NR_renameat2)
    #if defined(__x86_64__)
        #define __NR_renameat2 316
    #elif defined(__aarch64__)
        #define __NR_renameat2 276
    #elif defined(__ppc64__)
        #define __NR_renameat2 357
    #elif defined(__riscv)
        #define __NR_renameat2 276
    #else
        #error "Unsupported architecture"
    #endif
#endif


namespace DB
{

static bool supportsRenameat2Impl()
{
    VersionNumber renameat2_minimal_version(3, 15, 0);
    VersionNumber linux_version(Poco::Environment::osVersion());
    return linux_version >= renameat2_minimal_version;
}

static bool renameat2(const std::string & old_path, const std::string & new_path, int flags)
{
    if (!supportsRenameat2())
        return false;
    if (old_path.empty() || new_path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot rename {} to {}: path is empty", old_path, new_path);

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
        throwFromErrno(fmt::format("Cannot rename {} to {} because the second path already exists", old_path, new_path), ErrorCodes::ATOMIC_RENAME_FAIL);
    if (errno == ENOENT)
        throwFromErrno(fmt::format("Paths cannot be exchanged because {} or {} does not exist", old_path, new_path), ErrorCodes::ATOMIC_RENAME_FAIL);
    throwFromErrnoWithPath(fmt::format("Cannot rename {} to {}", old_path, new_path), new_path, ErrorCodes::SYSTEM_ERROR);
}

bool supportsRenameat2()
{
    static bool supports = supportsRenameat2Impl();
    return supports;
}

}

#else

#define RENAME_NOREPLACE -1
#define RENAME_EXCHANGE -1

namespace DB
{

static bool renameat2(const std::string &, const std::string &, int)
{
    return false;
}

bool supportsRenameat2()
{
    return false;
}

}

#endif

namespace DB
{

static void renameNoReplaceFallback(const std::string & old_path, const std::string & new_path)
{
    /// NOTE it's unsafe
    if (fs::exists(new_path))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File {} exists", new_path);
    fs::rename(old_path, new_path);
}

/// Do not use [[noreturn]] to avoid warnings like "code will never be executed" in other places
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-noreturn"
static void renameExchangeFallback(const std::string &, const std::string &)
{
    throw Exception("System call renameat2() is not supported", ErrorCodes::UNSUPPORTED_METHOD);
}
#pragma GCC diagnostic pop

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
