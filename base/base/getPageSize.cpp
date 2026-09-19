#include <base/getPageSize.h>
#include <cstdlib>

#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#else
#include <unistd.h>
#endif

namespace
{
    Int64 getPageSizeImpl()
    {
#if defined(OS_WINDOWS)
        SYSTEM_INFO info;
        GetSystemInfo(&info);
        /// Note that this is the page size, not `dwAllocationGranularity` - the 64 KiB boundary
        /// that `MapViewOfFile` requires. Callers that map files need the latter; see
        /// `MMappedFileDescriptor`.
        return info.dwPageSize;
#else
        Int64 page_size = sysconf(_SC_PAGESIZE);
        if (page_size < 0)
            abort();
        return page_size;
#endif
    }
}

Int64 getPageSize()
{
    static const Int64 page_size = getPageSizeImpl();
    return page_size;
}
