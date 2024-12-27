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

#include <Common/logger_useful.h>
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

static constexpr auto filename = "/proc/self/status";

MemoryStatisticsOS::MemoryStatisticsOS()
{
    fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, filename, "Cannot open file {}", filename);
}

MemoryStatisticsOS::~MemoryStatisticsOS()
{
    if (0 != ::close(fd))
    {
        try
        {
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_CLOSE_FILE, filename, "File descriptor for '{}' could not be closed", filename);
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

    constexpr size_t buf_size = 2048;
    char buf[buf_size];

    ssize_t res = 0;

    do
    {
        res = ::pread(fd, buf, buf_size, 0);

        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            ErrnoException::throwFromPath(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, filename, "Cannot read from file {}", filename);
        }

        assert(res >= 0);
        break;
    } while (true);

    ReadBufferFromMemory in(buf, res);

    data.virt = 0;
    data.resident = 0;
    data.swap = 0;
    data.shared = 0;
    data.code = 0;
    data.data_and_stack = 0;

    while (!in.eof()) {
        String name;
        readString(name, in);
        skipWhitespaceIfAny(in);
        if (name == "VmSize:")
        {
            readIntText(data.virt, in);
        }
        else if (name == "VmRSS:")
        {
            readIntText(data.resident, in);
        }
        else if (name == "VmSwap:")
        {
            readIntText(data.swap, in);
        }
        else if (name == "RssFile:" || name == "RssShmem:")
        {
            uint64_t rss_file_or_shmem;
            readIntText(rss_file_or_shmem, in);
            data.shared += rss_file_or_shmem;
        }
        else if (name == "VmData:" || name == "VmStk:")
        {
            uint64_t vm_data_or_stack;
            readIntText(vm_data_or_stack, in);
            data.data_and_stack += vm_data_or_stack;
        }
        else if (name == "VmExe:" || name == "VmLib:")
        {
            uint64_t vm_exe_or_lib;
            readIntText(vm_exe_or_lib, in);
            data.code += vm_exe_or_lib;
        }
        skipToNextLineOrEOF(in);
    }

    // Everything is in Kb
    data.virt *= 1024;
    data.resident *= 1024;
    data.swap *= 1024;
    data.shared *= 1024;
    data.code *= 1024;
    data.data_and_stack *= 1024;

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

    if (-1 == ::sysctl(mib, 4, &kp, &len, nullptr, 0))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot sysctl(kern.proc.pid.{})", std::to_string(self));

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
