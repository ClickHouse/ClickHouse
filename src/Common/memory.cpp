#include <sys/mman.h>
#include <Poco/Environment.h>
#include <Common/Exception.h>
#include <Common/VersionNumber.h>
#include <Common/memory.h>

#ifdef OS_LINUX
#if !defined(MADV_GUARD_INSTALL)
#define MADV_GUARD_INSTALL 102
#endif

#if !defined(MADV_GUARD_REMOVE)
#define MADV_GUARD_REMOVE 103
#endif

static bool supportsGuardPages()
{
    DB::VersionNumber madv_guard_minimal_version(6, 13, 0);
    DB::VersionNumber linux_version(Poco::Environment::osVersion());
    return (linux_version >= madv_guard_minimal_version);
}
static bool supports_guard_pages = supportsGuardPages();
#endif // OS_LINUX

namespace DB::ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

/// Uses MADV_GUARD_INSTALL if available, or mprotect() if not
void memoryGuardInstall(void *addr, size_t len)
{
#ifdef OS_LINUX
    if (supports_guard_pages)
    {
        if (madvise(addr, len, MADV_GUARD_INSTALL))
            throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "Cannot madvise(MADV_GUARD_INSTALL)");
    }
    else
#endif
    {
        if (mprotect(addr, len, PROT_NONE))
            throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "Cannot mprotect(PROT_NONE)");
    }
}

/// Uses MADV_GUARD_REMOVE if available, or mprotect() if not
void memoryGuardRemove(void *addr, size_t len)
{
#ifdef OS_LINUX
    if (supports_guard_pages)
    {
        if (madvise(addr, len, MADV_GUARD_REMOVE))
            throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "Cannot madvise(MADV_GUARD_INSTALL)");
    }
    else
#endif
    {
        if (mprotect(addr, len, PROT_READ|PROT_WRITE))
            throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "Cannot mprotect(PROT_NONE)");
    }
}
