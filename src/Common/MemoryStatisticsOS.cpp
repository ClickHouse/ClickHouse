#if defined(OS_LINUX) || defined(OS_FREEBSD)

#include <sys/types.h>
#include <sys/stat.h>
#if defined(OS_FREEBSD)
#include <sys/sysctl.h>
#include <sys/user.h>
#endif
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#include "MemoryStatisticsOS.h"

#include <base/logger_useful.h>
#include <base/getPageSize.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


namespace DB
{

#if defined(OS_LINUX)

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
}

static constexpr auto filename = "/proc/self/statm";

MemoryStatisticsOS::MemoryStatisticsOS()
{
    fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + std::string(filename), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

MemoryStatisticsOS::~MemoryStatisticsOS()
{
    if (0 != ::close(fd))
    {
        try
        {
            throwFromErrno(
                    "File descriptor for \"" + std::string(filename) + "\" could not be closed. "
                    "Something seems to have gone wrong. Inspect errno.", ErrorCodes::CANNOT_CLOSE_FILE);
        }
        catch (const ErrnoException &)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

MemoryStatisticsOS::Data MemoryStatisticsOS::get() const
{
    Data data;

    constexpr size_t buf_size = 1024;
    char buf[buf_size];

    ssize_t res = 0;

    do
    {
        res = ::pread(fd, buf, buf_size, 0);

        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            throwFromErrno("Cannot read from file " + std::string(filename), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        assert(res >= 0);
        break;
    } while (true);

    ReadBufferFromMemory in(buf, res);

    uint64_t unused;
    readIntText(data.virt, in);
    skipWhitespaceIfAny(in);
    readIntText(data.resident, in);
    skipWhitespaceIfAny(in);
    readIntText(data.shared, in);
    skipWhitespaceIfAny(in);
    readIntText(data.code, in);
    skipWhitespaceIfAny(in);
    readIntText(unused, in);
    skipWhitespaceIfAny(in);
    readIntText(data.data_and_stack, in);

    size_t page_size = static_cast<size_t>(::getPageSize());
    data.virt *= page_size;
    data.resident *= page_size;
    data.shared *= page_size;
    data.code *= page_size;
    data.data_and_stack *= page_size;

    return data;
}

#endif

#if defined(OS_FREEBSD)

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

MemoryStatisticsOS::MemoryStatisticsOS()
{
    pagesize = static_cast<size_t>(::getPageSize());
    self = ::getpid();
}

MemoryStatisticsOS::~MemoryStatisticsOS()
{
}

MemoryStatisticsOS::Data MemoryStatisticsOS::get() const
{
    Data data;
    int mib[4] = { CTL_KERN, KERN_PROC, KERN_PROC_PID, self };
    struct kinfo_proc kp;
    size_t len = sizeof(struct kinfo_proc);

    if (-1 == ::sysctl(mib, 4, &kp, &len, NULL, 0))
        throwFromErrno("Cannot sysctl(kern.proc.pid." + std::to_string(self) + ")", ErrorCodes::SYSTEM_ERROR);

    if (sizeof(struct kinfo_proc) != len)
        throw DB::Exception(DB::ErrorCodes::SYSTEM_ERROR, "Kernel returns structure of {} bytes instead of expected {}",
            len, sizeof(struct kinfo_proc));

    if (sizeof(struct kinfo_proc) != kp.ki_structsize)
        throw DB::Exception(DB::ErrorCodes::SYSTEM_ERROR, "Kernel structure size ({}) does not match expected ({}).",
            kp.ki_structsize, sizeof(struct kinfo_proc));

    data.virt = kp.ki_size;
    data.resident = kp.ki_rssize * pagesize;
    data.code = kp.ki_tsize * pagesize;
    data.data_and_stack = (kp.ki_dsize + kp.ki_ssize) * pagesize;

    return data;
}

#endif

}

#endif
