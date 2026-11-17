#pragma once

#if defined(OS_LINUX)

#include <cstdint>
#include <string>

#include <sys/types.h>

namespace DB
{

/// The parts of `isolateFilesystem` that run the server in a new PID namespace under a supervisor outside of it, when
/// the process has `CAP_SYS_ADMIN`: see `isolateFilesystem` and `notifyFilesystemIsolationSupervisor` for what each
/// process does.

/// Why the supervisor cannot run here, or empty.
std::string getSupervisorUnavailableReason();

/// Creates a child process in new PID and mount namespaces, as `fork` does: it returns twice. The current process
/// stays in its namespaces. The child has a single thread, like the current process.
pid_t cloneIntoPidAndMountNamespaces();

/// The current process stays outside as the supervisor, owns `pid_file` (if not empty), creates the init of new PID
/// and mount namespaces, which forks the server, and never returns. Returns in the server only, in the new
/// namespaces, with nothing mounted yet.
void startSupervisedServer(const std::string & pid_file);

/// Leaves only the capabilities in `keep`, and sets `PR_SET_NO_NEW_PRIVS`.
void restrictCapabilities(uint64_t keep);

/// Saves the PID of the process that serves outside of the PID namespace in the status of `isolateFilesystem`.
void setFilesystemIsolationServingHostPid(int pid);

}

#endif
