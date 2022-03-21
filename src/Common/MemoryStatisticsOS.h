#pragma once
#if defined(OS_LINUX) || defined(OS_FREEBSD)
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
        uint64_t code;
        uint64_t data_and_stack;
    };

    MemoryStatisticsOS();
    ~MemoryStatisticsOS();

    /// Thread-safe.
    Data get() const;

private:
#if defined(OS_LINUX)
    int fd;
#endif
#if defined(OS_FREEBSD)
    int pagesize;
    pid_t self;
#endif
};

}

#endif
