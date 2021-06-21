#if defined(OS_LINUX)

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#include "MemoryStatisticsOS.h"

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
}

static constexpr auto filename = "/proc/self/statm";
static constexpr size_t PAGE_SIZE = 4096;

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

    data.virt *= PAGE_SIZE;
    data.resident *= PAGE_SIZE;
    data.shared *= PAGE_SIZE;
    data.code *= PAGE_SIZE;
    data.data_and_stack *= PAGE_SIZE;

    return data;
}

}

#endif
