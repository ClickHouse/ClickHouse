#pragma once

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_SUNOS) || defined(OS_DARWIN)
#    define HAS_MEMORY_STATISTICS_OS 1
#else
#    define HAS_MEMORY_STATISTICS_OS 0
#endif

#if HAS_MEMORY_STATISTICS_OS
#include <cstdint>
#if defined(OS_FREEBSD)
#include <unistd.h>
#endif


namespace DB
{

/** Opens a file /proc/self/statm. Keeps it open and reads memory statistics via 'pread'.
  * This is Linux specific.
  * See: man procfs
  *
  * Note: a class is used instead of a single function to avoid excessive file open/close on every use.
  * pread is used to avoid lseek.
  *
  * Actual performance is from 1 to 5 million iterations per second.
  */
class MemoryStatisticsOS
{
public:
    /// In number of bytes.
    struct Data
    {
        uint64_t virt;
        uint64_t resident;
#if defined(OS_LINUX)
        uint64_t shared;
#endif
#if !defined(OS_DARWIN)
        uint64_t code;
        uint64_t data_and_stack;
#endif
    };

#if defined(OS_DARWIN)
    MemoryStatisticsOS() = default;
    ~MemoryStatisticsOS() = default;
#else
    MemoryStatisticsOS();
    ~MemoryStatisticsOS();
#endif

    /// Thread-safe.
    [[nodiscard]] Data get() const;

private:
#if defined(OS_LINUX)
    int fd;
#endif
#if defined(OS_FREEBSD)
    size_t pagesize;
    pid_t self;
#endif
#if defined(OS_SUNOS)
    int psinfo_fd;
#endif
};

}

#endif
