#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace DB
{

/// What the kernel does when the server - or any process it forks - issues a system call that the
/// seccomp policy does not allow. Controlled by the `seccomp` server setting.
///
/// A seccomp filter can never be removed or relaxed once installed, and it is inherited across both
/// `clone` and `execve`. Everything the server spawns (executable dictionaries and user defined
/// functions, the library and ODBC bridges, the OOM canary) therefore runs under the same policy.
enum class SeccompMode : uint8_t
{
    /// Do not install a filter at all.
    Disabled,
    /// Let the system call through and ask the kernel to record it in the audit log. No system
    /// call is refused in this mode; it exists to validate the policy against a real workload.
    /// `PR_SET_NO_NEW_PRIVS` is still set, because the kernel asks for it before it accepts a
    /// filter, so a setuid program the server runs still does not get to elevate.
    Log,
    /// Fail the system call with `EPERM`.
    Errno,
    /// Deliver `SIGSYS` to the offending thread. ClickHouse handles it like any other fatal signal:
    /// the log gets the system call number and a stack trace, and the process terminates.
    Trap,
    /// Kill the whole process immediately. Nothing is logged by the process itself, because no
    /// signal handler runs.
    Kill,
};

#if defined(OS_LINUX)

struct SeccompFilterStatus
{
    /// The number of allowed system calls, or 0 if no filter was installed.
    size_t allowed_syscalls = 0;
    /// Why no filter was installed although `mode` asked for one. Empty if a filter was installed,
    /// or if `mode` is `Disabled`.
    std::string not_installed_reason;
};

/// Installs a seccomp-BPF system call filter on every thread of the current process, allowing only
/// the system calls ClickHouse is known to use and applying `mode` to all the others.
///
/// In every mode but `Disabled` this also sets `PR_SET_NO_NEW_PRIVS`, which does not depend on the
/// architecture and happens even where no filter can be installed.
///
/// No filter is installed if `mode` is `Disabled`, if the policy is not implemented for this
/// architecture (only x86-64 and AArch64 are covered), or if `mode` is `Log` and the kernel cannot
/// install a filter with that action: the `Log` mode refuses nothing, so running without it takes
/// away nothing a filter would have enforced, and a server that used to start must not stop
/// starting because of it. Throws if one of the enforcing modes was requested but its filter could
/// not be installed.
///
/// Call this once, as early in the startup as the configuration allows: the filter takes effect
/// immediately and there is no way to widen it afterwards.
SeccompFilterStatus installSeccompFilter(SeccompMode mode);

#endif

}
