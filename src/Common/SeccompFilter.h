#pragma once

#include <cstddef>
#include <cstdint>

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
    /// Let the system call through and ask the kernel to record it in the audit log. Nothing is
    /// blocked in this mode; it exists to validate the policy against a real workload.
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

/// Installs a seccomp-BPF system call filter on every thread of the current process, allowing only
/// the system calls ClickHouse is known to use and applying `mode` to all the others.
///
/// Returns the number of allowed system calls, or 0 if no filter was installed - either because
/// `mode` is `Disabled`, or because the policy is not implemented for this architecture (only
/// x86-64 and AArch64 are covered). Throws if a filter was requested but could not be installed.
///
/// Call this once, as early in the startup as the configuration allows: the filter takes effect
/// immediately and there is no way to widen it afterwards.
size_t installSeccompFilter(SeccompMode mode);

#endif

}
