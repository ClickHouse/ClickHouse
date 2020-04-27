#include "ProcfsMetricsProvider.h"

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <cassert>
#include <sys/types.h>
#include <fcntl.h>
#include <linux/taskstats.h>

#include <cstdio>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

static constexpr auto thread_schedstat = "/proc/thread-self/schedstat";
static constexpr auto thread_stat = "/proc/thread-self/stat";
static constexpr auto thread_io = "/proc/thread-self/io";


namespace
{
[[noreturn]] inline static void throwWithFailedToOpenFile(const std::string & filename)
{
    throwFromErrno(
            "Cannot open file " + filename,
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

ssize_t readFromFD(const int fd, const char * filename, char * buf, size_t buf_size)
{
    ssize_t res = 0;

    do
    {
        res = ::pread(fd, buf, buf_size, 0);

        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            throwFromErrno(
                    "Cannot read from file " + std::string(filename),
                    ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        assert(res >= 0);
        break;
    } while (true);

    return res;
}
}


bool ProcfsMetricsProvider::isAvailable() {
    /// TODO: Add a simple feature test
    int fd = ::open(thread_schedstat, O_RDONLY);
    if (-1 == fd)
        return false;

    ::close(fd);
    return true;
}


ProcfsMetricsProvider::ProcfsMetricsProvider(const pid_t /*tid*/)
{
    thread_schedstat_fd = ::open(thread_schedstat, O_RDONLY | O_CLOEXEC);
    if (-1 == thread_schedstat_fd)
    {
        throwWithFailedToOpenFile(thread_schedstat);
    }
    thread_stat_fd = ::open(thread_stat, O_RDONLY | O_CLOEXEC);
    if (-1 == thread_stat_fd)
    {
        ::close(thread_schedstat_fd);
        throwWithFailedToOpenFile(thread_stat);
    }
    thread_io_fd = ::open(thread_io, O_RDONLY | O_CLOEXEC);
    if (-1 != thread_io_fd)
    {
        stats_version = 3;
    }
}


ProcfsMetricsProvider::~ProcfsMetricsProvider()
{
    if (stats_version >= 3 && 0 != ::close(thread_io_fd))
        tryLogCurrentException(__PRETTY_FUNCTION__);
    if (0 != ::close(thread_stat_fd))
        tryLogCurrentException(__PRETTY_FUNCTION__);
    if (0 != ::close(thread_schedstat_fd))
        tryLogCurrentException(__PRETTY_FUNCTION__);
}


void ProcfsMetricsProvider::getTaskStats(::taskstats & out_stats) const
{
    constexpr size_t buf_size = 1024;
    char buf[buf_size];

    out_stats.version = stats_version;

    readParseAndSetThreadCPUStat(out_stats, buf, buf_size);
    readParseAndSetThreadBlkIOStat(out_stats, buf, buf_size);

    if (stats_version >= 3)
    {
        readParseAndSetThreadIOStat(out_stats, buf, buf_size);
    }
}


void ProcfsMetricsProvider::readParseAndSetThreadCPUStat(::taskstats & out_stats, char * buf, size_t buf_size) const
{
    ssize_t res = readFromFD(thread_schedstat_fd, thread_schedstat, buf, buf_size);
    buf[res] = '\0';

    std::sscanf(buf, "%llu %llu", &out_stats.cpu_run_virtual_total, &out_stats.cpu_delay_total);
}


void ProcfsMetricsProvider::readParseAndSetThreadBlkIOStat(::taskstats & out_stats, char * buf, size_t buf_size) const
{
    ssize_t res = readFromFD(thread_stat_fd, thread_stat, buf, buf_size - 1);
    buf[res] = '\0';

    std::sscanf(
            buf,
            "%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s""%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s"
            "%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s""%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s"
            "%*s%llu",  /// Read field #42 - Aggregated block I/O delays, measured in clock ticks (centiseconds)
            &out_stats.blkio_delay_total);

    out_stats.blkio_delay_total *= 10000000ul;
}


void ProcfsMetricsProvider::readParseAndSetThreadIOStat(::taskstats & out_stats, char * buf, size_t buf_size) const
{
    ssize_t res = readFromFD(thread_io_fd, thread_io, buf, buf_size);
    ReadBufferFromMemory in_thread_io(buf, res);

    assertString("rchar:", in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    readIntText(out_stats.read_char, in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    assertString("wchar:", in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    readIntText(out_stats.write_char, in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    skipToNextLineOrEOF(in_thread_io);
    skipToNextLineOrEOF(in_thread_io);
    assertString("read_bytes:", in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    readIntText(out_stats.read_bytes, in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    assertString("write_bytes:", in_thread_io);
    skipWhitespaceIfAny(in_thread_io);
    readIntText(out_stats.write_bytes, in_thread_io);
}
}
