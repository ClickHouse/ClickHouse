#include <Common/OOMCanaryExitCodes.h>

#if defined(OS_LINUX)

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cerrno>


namespace
{

[[noreturn]] void runCanary(size_t size_bytes, pid_t parent_pid)
{
    /// Ask the kernel to send SIGKILL when the parent exits, so the canary
    /// cannot outlive the server.
    if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0)
        ::_exit(DB::OOMCanaryExitCodes::PERMANENT);

    /// Close the race between exec and prctl: if the parent already exited,
    /// getppid returns the new (subreaper) pid instead of the original.
    if (::getppid() != parent_pid)
        ::_exit(DB::OOMCanaryExitCodes::PERMANENT);

    /// Drop any non-CLOEXEC fds inherited across exec.
#ifdef __NR_close_range
    (void)::syscall(__NR_close_range, 3U, ~0U, 0);
#else
    int max_fd = static_cast<int>(::sysconf(_SC_OPEN_MAX));
    if (max_fd < 0 || max_fd > 65536)
        max_fd = 65536;
    for (int fd = 3; fd < max_fd; ++fd)
        (void)::close(fd);
#endif

    /// Make the OOM killer pick us first.
    try
    {
        DB::WriteBufferFromFile out("/proc/self/oom_score_adj", DB::DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY);
        DB::writeString("1000", out);
        out.finalize();
    }
    catch (const std::exception &)
    {
        ::_exit(DB::OOMCanaryExitCodes::PERMANENT);
    }

    void * mem = ::mmap(nullptr, size_bytes, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED)
    {
        ::_exit(DB::OOMCanaryExitCodes::TRANSIENT);
    }

    /// Force physical allocation: mmap only reserves virtual address space;
    /// pages remain COW-mapped to the zero page until first write.
#ifdef MADV_POPULATE_WRITE
    if (::madvise(mem, size_bytes, MADV_POPULATE_WRITE))
        ::_exit(DB::OOMCanaryExitCodes::TRANSIENT);
#else
    int64_t page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0)
        page_size = 4096;
    char * ptr = static_cast<char *>(mem);
    for (size_t offset = 0; offset < size_bytes; offset += static_cast<size_t>(page_size))
        ptr[offset] = static_cast<char>(offset & 0xFF);
#endif

    /// Best effort: keep pages resident so RSS stays predictable for the OOM
    /// killer's heuristic. Requires CAP_IPC_LOCK or sufficient RLIMIT_MEMLOCK;
    /// if unavailable, the pages remain allocated but become swap candidates.
    ::mlock(mem, size_bytes);

    /// Block all signals before pause so only SIGKILL (unblockable) can
    /// terminate us — from the OOM killer, from parent death (pdeathsig),
    /// or from the parent's explicit shutdown.
    sigset_t full_mask;
    ::sigfillset(&full_mask); // NOLINT(concurrency-mt-unsafe)
    ::sigprocmask(SIG_SETMASK, &full_mask, nullptr); // NOLINT(concurrency-mt-unsafe)

    for (;;)
        ::pause();
}

}


int mainEntryClickHouseOomCanary(int argc, char ** argv)
{
    if (argc != 3)
        return 1;

    size_t size_bytes = 0;
    pid_t parent_pid = 0;
    if (!DB::tryParse(size_bytes, argv[1]) || !DB::tryParse(parent_pid, argv[2]))
        return 1;

    runCanary(size_bytes, parent_pid);
}

#else

int mainEntryClickHouseOomCanary(int /*argc*/, char ** /*argv*/)
{
    return DB::OOMCanaryExitCodes::PERMANENT;
}

#endif
