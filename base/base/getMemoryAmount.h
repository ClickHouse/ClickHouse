#pragma once

#include <cstdint>

#if defined(OS_WINDOWS)
#include <optional>
#endif

/// Returns the size in bytes of physical memory (RAM) available to the process. The value can
/// be smaller than the total available RAM available to the system due to cgroups settings.
/// Returns 0 on unsupported platform or if it cannot determine the size of physical memory.
uint64_t getMemoryAmountOrZero();

/// Throws exception if it cannot determine the size of physical memory.
uint64_t getMemoryAmount();

#if defined(OS_WINDOWS)
std::optional<uint64_t> windowsJobObjectMemoryLimit(uint32_t limit_flags, uint64_t job_memory_limit, uint64_t process_memory_limit);
/// The memory limit a job object imposes, decided from the fields of
/// `JOBOBJECT_EXTENDED_LIMIT_INFORMATION`, or nothing when it imposes none. Windows has no
/// cgroups; this is what a container caps a process with.
///
/// Declared apart from the query that reads those fields so that the decision can be exercised
/// on its own: Wine, which is where CI runs the Windows binary, stubs
/// `JobObjectExtendedLimitInformation` - it zeroes the caller's struct and reports success,
/// see `NtQueryInformationJobObject` in its `dlls/ntdll/unix/sync.c` - so a test there cannot
/// impose a real limit and then observe it. See `utils/windows-selftest`.
#endif
