#include <Common/atomicRename.h>
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


#if defined(OS_LINUX)

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
    #elif defined(__powerpc64__)
        #define __NR_renameat2 357
    #elif defined(__riscv)
        #define __NR_renameat2 276
    #elif defined(__loongarch64)
        #define __NR_renameat2 276
    #else
        #error "Unsupported architecture"
    #endif
#endif


namespace DB
{

static std::optional<std::string> supportsAtomicRenameImpl()
{
    VersionNumber renameat2_minimal_version(3, 15, 0);
    VersionNumber linux_version(Poco::Environment::osVersion());
    if (linux_version >= renameat2_minimal_version)
        return std::nullopt;
    return fmt::format("Linux kernel 3.15+ is required, got {}", linux_version.toString());
}

static bool renameat2(const std::string & old_path, const std::string & new_path, int flags)
{
    if (!supportsAtomicRename())
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
        throw ErrnoException(
            ErrorCodes::ATOMIC_RENAME_FAIL, "Cannot rename {} to {} because the second path already exists", old_path, new_path);
    if (errno == ENOENT)
        throw ErrnoException(
            ErrorCodes::ATOMIC_RENAME_FAIL, "Paths cannot be exchanged because {} or {} does not exist", old_path, new_path);
    ErrnoException::throwFromPath(ErrorCodes::SYSTEM_ERROR, new_path, "Cannot rename {} to {}", old_path, new_path);
}

bool supportsAtomicRename(std::string * out_message)
{
    static auto error = supportsAtomicRenameImpl();
    if (!error.has_value())
        return true;
    if (out_message)
        *out_message = error.value();
    return false;
}

}

#elif defined(OS_DARWIN)

// Includes
#include <dlfcn.h>  // For dlsym
#include <stdio.h>  // For renamex_np
#include <string.h> // For stderror

#ifndef RENAME_SWAP
    #define RENAME_SWAP 0x00000002
#endif
#ifndef RENAME_EXCL
    #define RENAME_EXCL 0x00000004
#endif


#define RENAME_NOREPLACE RENAME_EXCL
#define RENAME_EXCHANGE RENAME_SWAP

namespace DB
{

static bool renameat2(const std::string & old_path, const std::string & new_path, int flags)
{
    using function_type = int (*)(const char * from, const char * to, unsigned int flags);
    static function_type fun = reinterpret_cast<function_type>(dlsym(RTLD_DEFAULT, "renamex_np"));
    if (fun == nullptr)
        return false;

    if (old_path.empty() || new_path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot rename {} to {}: path is empty", old_path, new_path);

    if (0 == (*fun)(old_path.c_str(), new_path.c_str(), flags))
        return true;
    int errnum = errno;

    if (errnum == ENOTSUP || errnum == EINVAL)
        return false;
    if (errnum == EEXIST)
        throw ErrnoException(
            ErrorCodes::ATOMIC_RENAME_FAIL, "Cannot rename {} to {} because the second path already exists", old_path, new_path);
    if (errnum == ENOENT)
        throw ErrnoException(
            ErrorCodes::ATOMIC_RENAME_FAIL, "Paths cannot be exchanged because {} or {} does not exist", old_path, new_path);
    ErrnoException::throwFromPath(ErrorCodes::SYSTEM_ERROR, new_path, "Cannot rename {} to {}", old_path, new_path);
}


static std::optional<std::string> supportsAtomicRenameImpl()
{
    auto fun = dlsym(RTLD_DEFAULT, "renamex_np");
    if (fun != nullptr)
        return std::nullopt;
    return "macOS 10.12 or later is required";
}

bool supportsAtomicRename(std::string * out_message)
{
    static auto error = supportsAtomicRenameImpl();
    if (!error.has_value())
        return true;
    if (out_message)
        *out_message = error.value();
    return false;
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

bool supportsAtomicRename(std::string * out_message)
{
    if (out_message)
        *out_message = "only Linux and macOS are supported";
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
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
static void renameExchangeFallback(const std::string &, const std::string &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "System call renameat2() is not supported");
}
#pragma clang diagnostic pop

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
