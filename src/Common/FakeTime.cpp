#include <Common/FakeTime.h>

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#endif

#if defined(__linux__) && defined(__x86_64__) && !defined(SPLIT_SHARED_LIBRARIES) && !defined(UNBUNDLED) && !defined(ARCADIA_BUILD)

#include <cassert>
#include <unistd.h>
#include <sys/syscall.h>
#include <Common/MemorySanitizer.h>


static int64_t offset = 0;


/// We cannot include stdlib.h and similar to avoid clashes with overridden function names.

extern "C" char * getenv(const char *);

namespace DB
{
    FakeTime::FakeTime()
    {
        const char * env = getenv("FAKE_TIME_OFFSET");
        if (!env)
            return;

        /// Manual and dirty parsing of the env variable to avoid including more headers.

        bool negative = false;
        while (*env)
        {
            if (*env == '-')
            {
                negative = true;
            }
            else
            {
                offset *= 10;
                offset += *env - '0';
            }
            ++env;
        }
        if (negative)
            offset = -offset;
    }

    bool FakeTime::isEffective()
    {
        return offset != 0;
    }
}


/// The constants and structure layout correspond to the ABI of Linux Kernel and libc on amd64.

extern "C"
{

#define CLOCK_REALTIME 0
#define CLOCK_REALTIME_COARSE 5
#define AT_FDCWD (-100)
#define AT_EMPTY_PATH 0x1000
#define AT_SYMLINK_NOFOLLOW 0x100

using time_t = int64_t;

struct Timespec
{
    time_t sec;
    long nsec;
};

struct Timeval
{
    time_t sec;
    long usec;
};


int __clock_gettime(int32_t clk_id, Timespec * tp); // NOLINT

int clock_gettime(int32_t clk_id, Timespec * tp)
{
    int res = __clock_gettime(clk_id, tp);

    if (0 == res)
    {
        __msan_unpoison(tp, sizeof(*tp));

        if (clk_id == CLOCK_REALTIME || clk_id == CLOCK_REALTIME_COARSE)
            tp->sec += offset;
    }

    return res;
}

int gettimeofday(Timeval * tv, void *)
{
    Timespec tp;
    int res = clock_gettime(CLOCK_REALTIME, &tp);
    if (0 == res)
    {
        tv->sec = tp.sec;
        tv->usec = tp.nsec / 1000;
    }
    return res;
}

time_t time(time_t * tloc)
{
    Timespec tp;
    int res = clock_gettime(CLOCK_REALTIME, &tp);
    (void)res;
    assert(0 == res);
    time_t t = tp.sec;
    if (tloc)
        *tloc = t;
    return t;
}

/// Filesystem time should be also altered as we sometimes compare it with the wall clock time.

struct Stat
{
    /// Make yourself confident:
    /// gcc -xc++ -include 'sys/stat.h' -include 'cstddef' - <<< "int main() { return offsetof(struct stat, st_atime); }" && ./a.out; echo $?
    char pad[72];

    Timespec st_atim;
    Timespec st_mtim;
    Timespec st_ctim;
};

static int fstatat(int fd, const char * pathname, Stat * statbuf, int flags)
{
    int res = syscall(__NR_newfstatat, fd, pathname, statbuf, flags);
    if (0 == res)
    {
        statbuf->st_atim.sec += offset;
        statbuf->st_mtim.sec += offset;
        statbuf->st_ctim.sec += offset;
    }
    return res;
}

int stat(const char * pathname, Stat * statbuf)
{
    return fstatat(AT_FDCWD, pathname, statbuf, 0);
}

int fstat(int fd, Stat * statbuf)
{
    return fstatat(fd, "", statbuf, AT_EMPTY_PATH);
}

int lstat(const char * pathname, Stat * statbuf)
{
    return fstatat(AT_FDCWD, pathname, statbuf, AT_SYMLINK_NOFOLLOW);
}

int stat64(const char * pathname, Stat * statbuf)
{
    return stat(pathname, statbuf);
}

int fstat64(int fd, Stat * statbuf)
{
    return fstat(fd, statbuf);
}

int fstatat64(int fd, const char * pathname, Stat * statbuf, int flags)
{
    return fstatat(fd, pathname, statbuf, flags);
}

int lstat64(const char * pathname, Stat * statbuf)
{
    return lstat(pathname, statbuf);
}

}

#else

namespace DB
{
    FakeTime::FakeTime()
    {
    }

    bool FakeTime::isEffective()
    {
        return false;
    }
}

#endif
