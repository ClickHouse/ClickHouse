#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_SUNOS)

#include <sys/types.h>
#include <sys/stat.h>
#if defined(OS_FREEBSD)
#include <sys/sysctl.h>
#include <sys/user.h>
#endif
#if defined(OS_SUNOS)
#include <procfs.h>
#endif
#include <fcntl.h>
#include <unistd.h>

#include <Common/MemoryStatisticsOS.h>

#include <Common/logger_useful.h>
#include <base/getPageSize.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
    extern const int SYSTEM_ERROR;
}

#if defined(OS_LINUX)

static constexpr auto filename = "/proc/self/statm";

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
    Data data{};

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

            ErrnoException::throwFromPath(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, filename, "Cannot read from file {}", filename);
        }

        chassert(res >= 0);
        break;
    } while (true);

    ReadBufferFromMemory in(buf, res);

    uint64_t unused = 0;
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

#if defined(OS_SUNOS)

static constexpr auto filename = "/proc/self/psinfo";

MemoryStatisticsOS::MemoryStatisticsOS()
{
    psinfo_fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == psinfo_fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, filename, "Cannot open file {}", filename);
}

MemoryStatisticsOS::~MemoryStatisticsOS()
{
    if (0 != ::close(psinfo_fd))
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
    psinfo_t info{};
    ssize_t bytes_read;

    do
    {
        bytes_read = ::pread(psinfo_fd, &info, sizeof(info), 0);
    } while (bytes_read == -1 && errno == EINTR);

    if (bytes_read == -1)
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, filename, "Cannot read from file {}", filename);

    if (static_cast<size_t>(bytes_read) != sizeof(info))
        throw DB::Exception(
            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
            "Cannot read from file {}: expected {} bytes, got {}",
            filename,
            sizeof(info),
            bytes_read);

    Data data{};
    data.virt = static_cast<uint64_t>(info.pr_size) * 1024;
    data.resident = static_cast<uint64_t>(info.pr_rssize) * 1024;

    return data;
}

#endif

}

#endif
