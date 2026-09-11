#include <Common/SeccompFilter.h>

#if defined(OS_LINUX)

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/types.h>

#include <asm/ioctls.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <span>
#include <unordered_set>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
}

#if defined(__x86_64__) || defined(__aarch64__)

/// The system calls that `clickhouse-server` - and every process it forks, which inherits the
/// filter - is allowed to make. Anything not named here gets the action configured by the
/// `seccomp` server setting.
///
/// The list is deliberately generous within each group: a system call that is merely an older or
/// a newer spelling of an allowed one (`open` next to `openat`, `epoll_wait` next to
/// `epoll_pwait2`, `clone` next to `clone3`) is allowed too, because which one is used depends on
/// the libc version and on the running kernel rather than on anything ClickHouse decides. Denying
/// those would turn a libc upgrade into a crash without taking away any capability the server does
/// not already have.
///
/// What the policy does take away is everything a healthy server never needs and an attacker who
/// achieved code execution inside the process would reach for: loading kernel modules
/// (`init_module`), rebooting (`reboot`, `kexec_load`), mounting and the whole new mount API
/// (`mount`, `pivot_root`, `fsopen`, `move_mount`), `chroot`, changing the process identity
/// (`setuid` and friends, `capset`), creating namespaces (`unshare`, `setns`), `ptrace` and
/// cross-process memory access (`process_vm_readv`, `process_vm_writev`, `pidfd_getfd`), eBPF
/// (`bpf`), the kernel keyring (`add_key`, `keyctl`), `userfaultfd` and `vmsplice` (both standard
/// exploitation primitives), file handles (`open_by_handle_at`), `fanotify`, swap and quota
/// control, setting the system clock and the host name, System V and POSIX IPC, and the
/// extended-attribute calls.
///
/// Two of the allowed calls are worth calling out, because they are the largest remaining surface
/// and both are here only because ClickHouse genuinely uses them: `perf_event_open` (for
/// `metrics_perf_events_enabled`) and the `io_uring` family (for
/// `local_filesystem_read_method = io_uring`).
/// clang-format off
#define SECCOMP_ALLOWED_SYSCALLS_COMMON(M) \
    /* Reading and writing data. */ \
    M(read) M(write) M(readv) M(writev) M(pread64) M(pwrite64) M(preadv) M(pwritev) \
    M(preadv2) M(pwritev2) M(lseek) M(sendfile) M(splice) M(copy_file_range) \
    \
    /* Opening, creating, renaming, removing and inspecting files. */ \
    M(openat) M(close) M(dup) M(dup3) M(fcntl) M(flock) \
    M(getdents64) M(mkdirat) M(unlinkat) M(renameat) M(renameat2) M(linkat) M(symlinkat) \
    M(readlinkat) M(mknodat) M(truncate) M(ftruncate) M(fallocate) M(fadvise64) M(readahead) \
    M(fchmod) M(fchmodat) M(fchown) M(fchownat) M(utimensat) M(faccessat) \
    M(fstat) M(newfstatat) M(statx) M(statfs) M(fstatfs) M(getcwd) M(chdir) \
    M(fchdir) M(umask) M(fsync) M(fdatasync) M(sync) M(syncfs) M(sync_file_range) \
    M(memfd_create) \
    \
    /* Device control. Which requests `ioctl` may carry is decided separately, in a block of the */ \
    /* program of its own, so this entry does not go into the table of numbers. */ \
    M(ioctl) \
    \
    /* Waiting for events, and file descriptors that carry them. */ \
    M(epoll_create1) M(epoll_ctl) M(epoll_pwait) M(ppoll) M(pselect6) M(pipe2) \
    M(eventfd2) M(signalfd4) M(timerfd_create) M(timerfd_settime) M(timerfd_gettime) \
    M(inotify_init1) M(inotify_add_watch) M(inotify_rm_watch) \
    \
    /* Networking. */ \
    M(socket) M(socketpair) M(bind) M(listen) M(accept) M(accept4) M(connect) M(shutdown) \
    M(getsockname) M(getpeername) M(setsockopt) M(getsockopt) M(sendto) M(recvfrom) \
    M(sendmsg) M(recvmsg) M(sendmmsg) M(recvmmsg) \
    \
    /* Memory. `get_mempolicy` is how `libnuma` reports which NUMA nodes the server may use. */ \
    M(mmap) M(munmap) M(mremap) M(mprotect) M(madvise) M(msync) M(mincore) M(brk) \
    M(mlock) M(mlock2) M(munlock) M(mlockall) M(munlockall) M(membarrier) M(get_mempolicy) \
    \
    /* Threads and processes. Forking and `execve` are needed by executable dictionaries and */ \
    /* user defined functions, by the bridges, and by the OOM canary. */ \
    M(clone) M(execve) M(execveat) M(exit) M(exit_group) M(wait4) M(waitid) \
    M(set_tid_address) M(set_robust_list) M(get_robust_list) M(setsid) M(setpgid) \
    M(getpgid) M(getsid) \
    \
    /* Synchronisation. */ \
    M(futex) \
    \
    /* Signals. `restart_syscall` is issued by the kernel itself and must never be blocked. */ \
    M(rt_sigaction) M(rt_sigprocmask) M(rt_sigreturn) M(rt_sigpending) M(rt_sigsuspend) \
    M(rt_sigtimedwait) M(rt_sigqueueinfo) M(rt_tgsigqueueinfo) M(sigaltstack) M(restart_syscall) \
    M(kill) M(tkill) M(tgkill) \
    \
    /* Time, including the timers behind the query profiler. */ \
    M(clock_gettime) M(clock_getres) M(gettimeofday) M(nanosleep) M(clock_nanosleep) \
    M(timer_create) M(timer_settime) M(timer_gettime) M(timer_getoverrun) M(timer_delete) \
    M(setitimer) M(getitimer) M(times) \
    \
    /* Scheduling and priorities. */ \
    M(sched_yield) M(sched_getaffinity) M(sched_setaffinity) M(sched_getattr) M(sched_setattr) \
    M(sched_getparam) M(sched_setparam) M(sched_getscheduler) M(sched_setscheduler) \
    M(sched_get_priority_max) M(sched_get_priority_min) M(sched_rr_get_interval) \
    M(setpriority) M(getpriority) M(getcpu) \
    \
    /* Information about the process and the system. */ \
    M(getpid) M(getppid) M(gettid) M(getuid) M(geteuid) M(getgid) M(getegid) M(getgroups) \
    M(getresuid) M(getresgid) M(capget) M(uname) M(sysinfo) M(getrusage) M(getrlimit) \
    M(setrlimit) M(prlimit64) M(prctl) M(getrandom) \
    \
    /* Asynchronous file reads through the older `libaio` read method. */ \
    M(io_setup) M(io_destroy) M(io_submit) M(io_cancel) M(io_getevents) \
    \
    /* Hardware performance counters, for `metrics_perf_events_enabled`. */ \
    M(perf_event_open)

#if defined(__x86_64__)
/// The x86-64 system call table kept the pre-`*at` forms of many calls, and which one a libc picks
/// for, say, `open` has changed more than once over the years. They do what their `*at`
/// counterparts above do.
#define SECCOMP_ALLOWED_SYSCALLS_ARCH(M) \
    M(open) M(creat) M(stat) M(lstat) M(access) M(pipe) M(poll) M(select) M(dup2) \
    M(getdents) M(rename) M(mkdir) M(rmdir) M(link) M(unlink) M(symlink) M(readlink) \
    M(chmod) M(chown) M(lchown) M(mknod) M(utime) M(utimes) M(futimesat) \
    M(epoll_create) M(epoll_wait) M(inotify_init) M(signalfd) M(eventfd) \
    M(alarm) M(pause) M(time) M(getpgrp) M(fork) M(vfork) \
    /* Thread-local storage setup, called by the libc on every thread. */ \
    M(arch_prctl)
#else
#define SECCOMP_ALLOWED_SYSCALLS_ARCH(M)
#endif

#if defined(SANITIZER)
/// The sanitizer runtimes need more than the server itself does: the leak detector stops the world
/// by attaching to every thread with `ptrace` and reads their memory from the outside.
#define SECCOMP_ALLOWED_SYSCALLS_SANITIZER(M) \
    M(ptrace) M(process_vm_readv) M(process_vm_writev) M(personality)
#else
#define SECCOMP_ALLOWED_SYSCALLS_SANITIZER(M)
#endif

/// Every system call above is named by the kernel headers in the build sysroot. These are not:
/// they are newer than those headers. The numbers are part of the kernel ABI, and from 424 onwards
/// a new system call gets the same number on every architecture by convention.
#define SECCOMP_ALLOWED_SYSCALLS_BY_NUMBER(N) \
    /* Registering the restartable sequences area, which a modern libc does on every thread. */ \
    N(rseq, rseq_syscall_number) \
    /* Watching and signalling a process by descriptor, used by the OOM canary. */ \
    N(pidfd_send_signal, 424) N(pidfd_open, 434) \
    /* The `io_uring` read method. */ \
    N(io_uring_setup, 425) N(io_uring_enter, 426) N(io_uring_register, 427) \
    /* Newer spellings of calls allowed above; which one is used is up to the libc. */ \
    N(clone3, 435) N(close_range, 436) N(openat2, 437) N(faccessat2, 439) N(epoll_pwait2, 441) \
    N(futex_waitv, 449) N(fchmodat2, 452) N(futex_wake, 454) N(futex_wait, 455) \
    N(futex_requeue, 456) \
    /* How a libc with shadow stacks enabled starts a thread, and how one seals its mappings. */ \
    N(map_shadow_stack, 453) N(mseal, 462)
/// clang-format on

namespace
{

/// `rseq` was added before the convention of giving a new system call the same number everywhere.
#if defined(__x86_64__)
constexpr int rseq_syscall_number = 334;
#else
constexpr int rseq_syscall_number = 293;
#endif

constexpr int allowed_syscalls[] =
{
#define SECCOMP_SYSCALL_BY_NAME(name) __NR_ ## name,
#define SECCOMP_SYSCALL_BY_NUMBER(name, number) number,
    SECCOMP_ALLOWED_SYSCALLS_COMMON(SECCOMP_SYSCALL_BY_NAME)
    SECCOMP_ALLOWED_SYSCALLS_ARCH(SECCOMP_SYSCALL_BY_NAME)
    SECCOMP_ALLOWED_SYSCALLS_SANITIZER(SECCOMP_SYSCALL_BY_NAME)
    SECCOMP_ALLOWED_SYSCALLS_BY_NUMBER(SECCOMP_SYSCALL_BY_NUMBER)
#undef SECCOMP_SYSCALL_BY_NAME
#undef SECCOMP_SYSCALL_BY_NUMBER
};

/// `ioctl` is allowed, but not for the two requests that let a process holding a terminal take
/// over the session that owns it: `TIOCSTI` pushes characters into the terminal's input queue as
/// if they had been typed, and `TIOCLINUX` can read back the contents of a virtual console.
/// Everything else `ioctl` can do is reachable through system calls that are allowed anyway.
constexpr UInt32 denied_ioctl_requests[] = {TIOCSTI, TIOCLINUX};

#if defined(__x86_64__)
constexpr UInt32 expected_audit_arch = AUDIT_ARCH_X86_64;
#else
constexpr UInt32 expected_audit_arch = AUDIT_ARCH_AARCH64;
#endif

/// Offsets of the `struct seccomp_data` fields the program loads.
constexpr UInt32 offset_nr = offsetof(struct seccomp_data, nr);
constexpr UInt32 offset_arch = offsetof(struct seccomp_data, arch);
/// The `request` argument of `ioctl`. The kernel narrows it to `unsigned int`, so only the low
/// half of the 64-bit argument decides what the call does - and on a little-endian machine that
/// half comes first. Looking at the low half alone is also what makes the check unavoidable:
/// passing `TIOCSTI` with junk in the high bits still reaches `TIOCSTI`, and still matches here.
constexpr UInt32 offset_ioctl_request = offsetof(struct seccomp_data, args) + sizeof(UInt64);

static_assert(
    std::endian::native == std::endian::little,
    "The seccomp filter reads the low half of a 64-bit system call argument, which assumes little-endian byte order");

using Program = std::vector<sock_filter>;

/// `BPF_STMT` and `BPF_JUMP` from `<linux/filter.h>` expand to C compound literals, which C++ does
/// not have, so the instructions are built here instead.
constexpr sock_filter statement(UInt32 code, UInt32 k)
{
    return sock_filter{.code = static_cast<UInt16>(code), .jt = 0, .jf = 0, .k = k};
}

constexpr sock_filter jump(UInt32 code, UInt32 k, UInt32 jt, UInt32 jf)
{
    return sock_filter{.code = static_cast<UInt16>(code), .jt = static_cast<UInt8>(jt), .jf = static_cast<UInt8>(jf), .k = k};
}

/// Jumps to the blocks at the end of the program. Their positions are not known while the body is
/// being emitted, so the jumps carry a placeholder distance that `link` replaces with the real
/// one. No real distance can collide with these: the program is a few hundred instructions long.
constexpr UInt32 jump_to_allow = 0xFFFFFFFFU;
constexpr UInt32 jump_to_deny = 0xFFFFFFFEU;
constexpr UInt32 jump_to_ioctl_check = 0xFFFFFFFDU;

/// A maximal run of consecutive allowed system call numbers, `[first, last]`.
struct Range
{
    int first;
    int last;
};

/// Splits sorted, deduplicated system call numbers into maximal runs of consecutive values, so
/// that the program compares against a few dozen ranges instead of a few hundred numbers.
std::vector<Range> toRanges(std::span<const int> sorted_numbers)
{
    std::vector<Range> ranges;
    for (int number : sorted_numbers)
    {
        if (!ranges.empty() && ranges.back().last + 1 == number)
            ranges.back().last = number;
        else
            ranges.push_back({.first = number, .last = number});
    }
    return ranges;
}

/// Emits the test of a single range, with the system call number already in the accumulator.
void emitRangeCheck(Program & program, const Range & range)
{
    if (range.first == range.last)
    {
        program.push_back(jump(BPF_JMP | BPF_JEQ | BPF_K, static_cast<UInt32>(range.first), 0, 1));
        program.push_back(statement(BPF_JMP | BPF_JA, jump_to_allow));
        program.push_back(statement(BPF_JMP | BPF_JA, jump_to_deny));
        return;
    }

    program.push_back(jump(BPF_JMP | BPF_JGT | BPF_K, static_cast<UInt32>(range.last), 0, 1));
    program.push_back(statement(BPF_JMP | BPF_JA, jump_to_deny));
    program.push_back(jump(BPF_JMP | BPF_JGE | BPF_K, static_cast<UInt32>(range.first), 0, 1));
    program.push_back(statement(BPF_JMP | BPF_JA, jump_to_allow));
    program.push_back(statement(BPF_JMP | BPF_JA, jump_to_deny));
}

/// Emits a binary search over `ranges`, which must be sorted and disjoint. A linear chain of
/// comparisons would work too, but the filter runs on every single system call the server makes,
/// so the depth is kept logarithmic: about a dozen instructions rather than a few hundred.
///
/// The comparisons are unsigned, and that is what makes a number outside the table - a negative
/// one, or an x32 number with bit 30 set - fall past the last range and into the deny block.
void emitSearch(Program & program, std::span<const Range> ranges)
{
    if (ranges.size() == 1)
    {
        emitRangeCheck(program, ranges.front());
        return;
    }

    const size_t middle = ranges.size() / 2;

    Program lower;
    emitSearch(lower, ranges.subspan(0, middle));
    Program upper;
    emitSearch(upper, ranges.subspan(middle));

    /// A conditional jump only reaches 255 instructions ahead, so it decides between falling
    /// through to the lower half and taking a `BPF_JA`, whose distance is 32 bits wide.
    program.push_back(jump(BPF_JMP | BPF_JGE | BPF_K, static_cast<UInt32>(ranges[middle].first), 0, 1));
    program.push_back(statement(BPF_JMP | BPF_JA, static_cast<UInt32>(lower.size())));
    program.insert(program.end(), lower.begin(), lower.end());
    program.insert(program.end(), upper.begin(), upper.end());
}

/// Replaces the placeholder distances with real ones.
void link(Program & program, size_t allow_index, size_t deny_index, size_t ioctl_check_index)
{
    for (size_t index = 0; index < program.size(); ++index)
    {
        sock_filter & instruction = program[index];
        if (instruction.code != (BPF_JMP | BPF_JA))
            continue;

        size_t target = 0;
        if (instruction.k == jump_to_allow)
            target = allow_index;
        else if (instruction.k == jump_to_deny)
            target = deny_index;
        else if (instruction.k == jump_to_ioctl_check)
            target = ioctl_check_index;
        else
            continue;

        /// Classic BPF jump distances are unsigned, so a target can only be placed after every
        /// jump to it - which is also what the kernel's filter verifier insists on.
        if (target <= index)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The generated seccomp filter jumps backwards, from instruction {} to {}",
                index,
                target);

        instruction.k = static_cast<UInt32>(target - index - 1);
    }
}

Program buildProgram(std::span<const Range> ranges, UInt32 default_action)
{
    if (ranges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The seccomp policy allows no system calls at all");

    Program program;

    /// A system call made through another architecture's ABI is rejected before its number is even
    /// looked at: the same number means something else there.
    program.push_back(statement(BPF_LD | BPF_W | BPF_ABS, offset_arch));
    program.push_back(jump(BPF_JMP | BPF_JEQ | BPF_K, expected_audit_arch, 1, 0));
    program.push_back(statement(BPF_JMP | BPF_JA, jump_to_deny));

    program.push_back(statement(BPF_LD | BPF_W | BPF_ABS, offset_nr));
    program.push_back(jump(BPF_JMP | BPF_JEQ | BPF_K, __NR_ioctl, 0, 1));
    program.push_back(statement(BPF_JMP | BPF_JA, jump_to_ioctl_check));

    emitSearch(program, ranges);

    /// The `ioctl` block comes before the two terminal blocks because it jumps to them.
    const size_t ioctl_check_index = program.size();
    program.push_back(statement(BPF_LD | BPF_W | BPF_ABS, offset_ioctl_request));
    for (UInt32 request : denied_ioctl_requests)
    {
        program.push_back(jump(BPF_JMP | BPF_JEQ | BPF_K, request, 0, 1));
        program.push_back(statement(BPF_JMP | BPF_JA, jump_to_deny));
    }
    program.push_back(statement(BPF_JMP | BPF_JA, jump_to_allow));

    const size_t allow_index = program.size();
    program.push_back(statement(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));
    const size_t deny_index = program.size();
    program.push_back(statement(BPF_RET | BPF_K, default_action));

    link(program, allow_index, deny_index, ioctl_check_index);
    return program;
}

/// An interpreter for the handful of instructions `buildProgram` emits. It exists so that the
/// program can be checked against the table it was generated from before the kernel is asked to
/// enforce it: a mistake in the generator would either open a hole or, far more likely, take the
/// server down the first time it makes a system call it is supposed to be allowed to make.
UInt32 evaluate(const Program & program, const struct seccomp_data & data)
{
    UInt32 accumulator = 0;
    size_t index = 0;
    while (index < program.size())
    {
        const sock_filter & instruction = program[index];

        if (instruction.code == (BPF_LD | BPF_W | BPF_ABS))
        {
            if (instruction.k + sizeof(accumulator) > sizeof(data))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "The generated seccomp filter reads past `seccomp_data` at offset {}",
                    instruction.k);
            memcpy(&accumulator, reinterpret_cast<const char *>(&data) + instruction.k, sizeof(accumulator));
            ++index;
        }
        else if (instruction.code == (BPF_RET | BPF_K))
            return instruction.k;
        else if (instruction.code == (BPF_JMP | BPF_JA))
            index += 1 + instruction.k;
        else if (instruction.code == (BPF_JMP | BPF_JEQ | BPF_K))
            index += 1 + (accumulator == instruction.k ? instruction.jt : instruction.jf);
        else if (instruction.code == (BPF_JMP | BPF_JGE | BPF_K))
            index += 1 + (accumulator >= instruction.k ? instruction.jt : instruction.jf);
        else if (instruction.code == (BPF_JMP | BPF_JGT | BPF_K))
            index += 1 + (accumulator > instruction.k ? instruction.jt : instruction.jf);
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected instruction {:#x} in the generated seccomp filter", instruction.code);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "The generated seccomp filter runs past its last instruction");
}

void verifyProgram(const Program & program, const std::unordered_set<int> & allowed, UInt32 default_action)
{
    auto check = [&](const struct seccomp_data & data, UInt32 expected)
    {
        const UInt32 result = evaluate(program, data);
        if (result == expected)
            return;

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The generated seccomp filter returns {:#x} instead of {:#x} for system call {} of architecture {:#x} "
            "with second argument {:#x}",
            result,
            expected,
            data.nr,
            data.arch,
            data.args[1]);
    };

    /// Every number the running kernel could put into `nr`, and then some, so that a range which
    /// is off by one on either end cannot go unnoticed.
    for (int nr = -4096; nr < 8192; ++nr)
        check(
            {.nr = nr, .arch = expected_audit_arch, .instruction_pointer = 0, .args = {}},
            allowed.contains(nr) ? SECCOMP_RET_ALLOW : default_action);

    /// The x32 ABI reports the same `arch` as x86-64 but sets bit 30 of the number, so its numbers
    /// must not be taken for the 64-bit ones.
    for (int nr : {__NR_read, __NR_mmap, __NR_ioctl})
        check({.nr = nr | 0x40000000, .arch = expected_audit_arch, .instruction_pointer = 0, .args = {}}, default_action);

    /// Another architecture's ABI, whatever the number.
    for (int nr : {__NR_read, __NR_ioctl, 0, 1000})
        check({.nr = nr, .arch = expected_audit_arch ^ 1U, .instruction_pointer = 0, .args = {}}, default_action);

    for (UInt32 request : denied_ioctl_requests)
    {
        check(
            {.nr = __NR_ioctl, .arch = expected_audit_arch, .instruction_pointer = 0, .args = {0, request, 0, 0, 0, 0}},
            default_action);
        /// The kernel narrows the request to 32 bits, and so must the filter.
        check(
            {.nr = __NR_ioctl,
             .arch = expected_audit_arch,
             .instruction_pointer = 0,
             .args = {0, UInt64(1) << 32 | request, 0, 0, 0, 0}},
            default_action);
    }

    for (UInt32 request : {UInt32{TIOCGWINSZ}, UInt32{FIONREAD}, UInt32{0}})
        check(
            {.nr = __NR_ioctl, .arch = expected_audit_arch, .instruction_pointer = 0, .args = {0, request, 0, 0, 0, 0}},
            SECCOMP_RET_ALLOW);
}

UInt32 getDefaultAction(SeccompMode mode)
{
    switch (mode)
    {
        case SeccompMode::Log:
            return SECCOMP_RET_LOG;
        case SeccompMode::Errno:
            return SECCOMP_RET_ERRNO | EPERM;
        case SeccompMode::Trap:
            return SECCOMP_RET_TRAP;
        case SeccompMode::Kill:
            return SECCOMP_RET_KILL_PROCESS;
        case SeccompMode::Disabled:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No seccomp action corresponds to the mode {}", static_cast<int>(mode));
}

/// The name the kernel uses for the action in `/proc/sys/kernel/seccomp/actions_avail`.
std::string_view getActionName(SeccompMode mode)
{
    switch (mode)
    {
        case SeccompMode::Log:
            return "log";
        case SeccompMode::Errno:
            return "errno";
        case SeccompMode::Trap:
            return "trap";
        case SeccompMode::Kill:
            return "kill_process";
        case SeccompMode::Disabled:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No seccomp action corresponds to the mode {}", static_cast<int>(mode));
}

/// A filter return value the running kernel does not implement is treated as `kill_process`, which
/// would silently turn `log` - the mode whose whole point is to change nothing - into the harshest
/// mode there is. So the action is checked against what the kernel says it implements.
void checkActionIsAvailable(SeccompMode mode)
{
    /// Added in Linux 4.14, together with the two newest actions asked for here.
    static constexpr auto path = "/proc/sys/kernel/seccomp/actions_avail";

    if (!std::filesystem::exists(path))
    {
        if (mode == SeccompMode::Log || mode == SeccompMode::Kill)
            throw Exception(
                ErrorCodes::SYSTEM_ERROR,
                "The `seccomp` server setting is set to `{}`, but the kernel has no {}, so it predates the Linux 4.14 that "
                "introduced this action. Use `trap` or `errno` instead, or set `seccomp` to `disabled`",
                getActionName(mode),
                path);
        return;
    }

    ReadBufferFromFile in(path);
    String available;
    readStringUntilEOF(available, in);

    const std::string_view action = getActionName(mode);
    bool found = false;
    for (size_t begin = 0; begin < available.size();)
    {
        const size_t end = std::min(available.find_first_of(" \t\n", begin), available.size());
        found |= std::string_view{available}.substr(begin, end - begin) == action;
        begin = end + 1;
    }

    if (!found)
        throw Exception(
            ErrorCodes::SYSTEM_ERROR,
            "The `seccomp` server setting is set to `{}`, but the only seccomp actions the kernel implements are: {}",
            action,
            available);
}

/// Calling `seccomp` with a null program is the documented way of asking whether a flag is
/// supported: the kernel validates the flags before it looks at the program, so an unsupported
/// flag gives `EINVAL` while a supported one gets as far as reading the program and gives `EFAULT`.
/// Nothing is installed either way.
bool isFilterFlagSupported(unsigned int flag)
{
    return -1 == syscall(__NR_seccomp, SECCOMP_SET_MODE_FILTER, flag, nullptr) && errno == EFAULT;
}

}

size_t installSeccompFilter(SeccompMode mode)
{
    if (mode == SeccompMode::Disabled)
        return 0;

    checkActionIsAvailable(mode);

    std::vector<int> numbers(std::begin(allowed_syscalls), std::end(allowed_syscalls));
    std::sort(numbers.begin(), numbers.end());
    numbers.erase(std::unique(numbers.begin(), numbers.end()), numbers.end());

    const std::unordered_set<int> allowed(numbers.begin(), numbers.end());

    /// `ioctl` is decided by request in a block of its own, so it must not also be in the table.
    std::erase(numbers, __NR_ioctl);

    const UInt32 default_action = getDefaultAction(mode);
    Program program = buildProgram(toRanges(numbers), default_action);
    verifyProgram(program, allowed, default_action);

    if (program.size() > size_t{BPF_MAXINSNS})
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The generated seccomp filter is {} instructions long, more than the {} the kernel accepts",
            program.size(),
            BPF_MAXINSNS);

    /// Installing a filter needs either this or `CAP_SYS_ADMIN`, and it is wanted in its own right:
    /// from here on, neither the server nor anything it forks can gain privileges by executing a
    /// setuid binary or one carrying file capabilities.
    if (0 != prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0))
        throw ErrnoException(
            ErrorCodes::SYSTEM_ERROR, "Cannot do `prctl(PR_SET_NO_NEW_PRIVS)`, which is required to install a seccomp filter");

    unsigned int flags = SECCOMP_FILTER_FLAG_TSYNC;

    /// Have the kernel record every refused system call in the audit log, naming the process and
    /// the system call number. This is the only evidence there is in the `kill` mode, where the
    /// server does not get to log anything, and it is what makes the `log` mode dependable: with
    /// this flag the record does not depend on the action being listed in
    /// `/proc/sys/kernel/seccomp/actions_logged`.
    if (isFilterFlagSupported(SECCOMP_FILTER_FLAG_LOG))
        flags |= SECCOMP_FILTER_FLAG_LOG;

    /// A seccomp filter makes the kernel force the Speculative Store Bypass mitigation on the
    /// process for the rest of its life unless this flag asks it not to, and that mitigation costs
    /// throughput on every query. ClickHouse does not run untrusted machine code in its own
    /// address space, so this policy is a barrier against a compromised server rather than a
    /// sandbox around hostile code, and paying for Spectre v4 hardening to get it is not the trade
    /// to make. The flag arrived in Linux 4.17.
    if (isFilterFlagSupported(SECCOMP_FILTER_FLAG_SPEC_ALLOW))
        flags |= SECCOMP_FILTER_FLAG_SPEC_ALLOW;

    /// `TSYNC` applies the filter to every thread of the process rather than only to this one.
    /// Without it the threads that already exist - the signal listener, the logging and the
    /// jemalloc background threads - would keep running unfiltered.
    const struct sock_fprog prog{.len = static_cast<UInt16>(program.size()), .filter = program.data()};
    const Int64 result = syscall(__NR_seccomp, SECCOMP_SET_MODE_FILTER, flags, &prog);
    if (result == -1)
        throw ErrnoException(
            ErrorCodes::SYSTEM_ERROR,
            "Cannot install the seccomp filter. If the kernel is built without `CONFIG_SECCOMP_FILTER`, or the `seccomp` "
            "system call is itself blocked by an outer sandbox, set the `seccomp` server setting to `disabled`");

    /// With `TSYNC` the kernel answers with the id of the first thread it could not synchronise,
    /// rather than with an error, and leaves no thread filtered at all in that case.
    if (result != 0)
        throw Exception(
            ErrorCodes::SYSTEM_ERROR,
            "Cannot install the seccomp filter on every thread of the process: thread {} could not be synchronized",
            result);

    return allowed.size();
}

#else

size_t installSeccompFilter(SeccompMode)
{
    /// A policy is a list of system call numbers, and those differ from one architecture to
    /// another; only x86-64 and AArch64 are covered. Returning zero is not an error - the caller
    /// reports that the server is running without a filter.
    return 0;
}

#endif

}

#endif
