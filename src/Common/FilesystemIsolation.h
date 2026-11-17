#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

/// Whether the server confines itself to a private view of the filesystem at startup, which contains only the
/// paths it needs. Controlled by the `filesystem_isolation` server setting.
enum class FilesystemIsolationMode : uint8_t
{
    /// Do not isolate the filesystem.
    Disabled,
    /// Isolate the filesystem if the kernel and the environment allow the process to create a mount namespace,
    /// and run without isolation (with a warning) if they do not, for example inside a container that forbids it.
    BestEffort,
    /// Isolate the filesystem, or refuse to start.
    Enforce,
};

/// A path that stays visible inside the isolated filesystem, at the same location as outside.
struct FilesystemIsolationPath
{
    enum class Access : uint8_t
    {
        /// Mounted read-only. Programs in it can be executed.
        ReadOnly,
        /// Mounted read-write. Nothing in it can be executed (`noexec`).
        ReadWrite,
    };

    /// An absolute path to a directory or a file. Symbolic links in it are recreated inside the isolated
    /// filesystem, and their targets are made visible as well.
    std::string path;
    Access access = Access::ReadOnly;
    /// Where the path comes from, for the log: a configuration key or a description.
    std::string source;
    /// A program that the server executes after the isolation: its dynamic loader and the shared libraries it needs
    /// are made visible as well.
    bool executable = false;
};

struct FilesystemIsolationStatus
{
    /// Whether the filesystem is isolated.
    bool isolated = false;
    /// Whether a user namespace had to be created, because the process did not have `CAP_SYS_ADMIN`.
    bool created_user_namespace = false;
    /// Whether the server runs in a new PID namespace, under a supervisor outside of it, because no user namespace was
    /// created.
    bool created_pid_namespace = false;
    /// The PID of the supervisor outside of the PID namespace of the isolation. It is the original process of the
    /// server: its PID is in the pid file and is the main PID for systemd. 0 without a PID namespace.
    int supervisor_pid = 0;
    /// Why the filesystem is not isolated although the mode asked for it. Empty if it is isolated or if the mode
    /// is `Disabled`.
    std::string not_isolated_reason;
    /// The capabilities of the initial user namespace that the process held before the isolation and does not
    /// have anymore, because the isolation had to create a user namespace.
    std::vector<std::string> lost_capabilities;
    /// The paths that are visible inside the isolated filesystem, with their access, for the log.
    std::vector<std::string> visible_paths;
};

/// Confines the server to a private view of the filesystem: a new mount namespace whose root is an empty read-only
/// `tmpfs`, with only the paths from `collect_paths` (and a few things every process needs: `/proc`, `/sys`, a handful
/// of devices in `/dev`, a private `/tmp`, the files mapped into the process, which include its executable)
/// bind-mounted into it, without the access to the original root. Then drops the capabilities that would let the
/// process undo it.
///
/// If the process has `CAP_SYS_ADMIN`, it keeps the capabilities it uses (such as `CAP_NET_BIND_SERVICE`,
/// `CAP_IPC_LOCK`, `CAP_SYS_NICE`, `CAP_NET_ADMIN`), and nothing but the kernel's checks of the user and the
/// capabilities would keep it from the files of another process of the same user through `/proc/<pid>/root`. So the
/// current process P stays outside, in the original namespaces, as a supervisor, and never returns from this function.
/// It creates a child C in new PID and mount namespaces. C is the init of the PID namespace: it forks the server S,
/// reaps the orphans, and exits with the status of S. S mounts a fresh `/proc` of the new PID namespace, so no process
/// outside of it is visible, isolates the filesystem and returns. P keeps the original PID: it writes it to `pid_file`
/// (if not empty), which S does not see, sends the signals it gets to S, relays a few validated notifications of S to
/// systemd (see `notifyFilesystemIsolationSupervisor`), and exits with the status of S, by the same signal if S was
/// killed by one. P drops all its capabilities and runs under a narrow seccomp filter.
///
/// Otherwise the current process creates new user and mount namespaces and stays the server, mapping only the current
/// user and group to themselves, and loses all its capabilities of the initial user namespace. The kernel does not let
/// it reach the files of any process outside of the user namespace through `/proc`, which is the original one, so the
/// processes outside are visible, but their files are not. It creates the pid file itself.
///
/// Must be called when the process has exactly one thread. Throws a `LOGICAL_ERROR` exception otherwise.
///
/// In the `BestEffort` mode, if the process is not allowed to create the namespaces or to mount the filesystems in
/// them (which is checked in a child process first), or if the pid file could be changed from inside, returns a
/// status with `isolated = false` and the reason, and nothing changes. Any later failure, for example a path that
/// cannot be mounted, stops the server in both modes. `collect_paths` is called only once the namespaces are known to
/// be available, with whether there is a supervisor, which then owns the pid file and the notifications of systemd.
using FilesystemIsolationPathsCollector = std::function<std::vector<FilesystemIsolationPath>(bool with_supervisor)>;
FilesystemIsolationStatus isolateFilesystem(
    FilesystemIsolationMode mode, const FilesystemIsolationPathsCollector & collect_paths, const std::string & pid_file = {});

/// The mode `isolateFilesystem` was called with, or nothing if it has not been called.
std::optional<FilesystemIsolationMode> getRequestedFilesystemIsolationMode();

/// The mode in force: the requested one if the filesystem is isolated, `Disabled` otherwise. Nothing if
/// `isolateFilesystem` has not been called.
std::optional<FilesystemIsolationMode> getAppliedFilesystemIsolationMode();

/// The status returned by `isolateFilesystem`, or nothing if it has not been called.
std::optional<FilesystemIsolationStatus> getFilesystemIsolationStatus();

/// Sends a notification for the service manager (such as `READY=1\n`) to the supervisor outside of the isolation,
/// which relays only `READY=1`, `STOPPING=1` and `STATUS=...` to the `NOTIFY_SOCKET` it was started with, from its own
/// PID, which systemd knows as the main one, and drops `MAINPID=`. Waits for its answer, and throws if it could not be
/// relayed. Returns false if the filesystem is not isolated, and the notification should be sent directly.
bool notifyFilesystemIsolationSupervisor(std::string_view message);

/// Asks the supervisor for the PID of the current process outside of the PID namespace of the isolation, which only
/// it can see. Nothing if the filesystem is not isolated or the supervisor does not answer.
std::optional<int> getFilesystemIsolationHostPid();

/// Makes the current process the one that the supervisor sends the signals to, which are sent to the PID in the pid
/// file. Called where the pid file used to be created: in the process that serves, after the fork of the watchdog.
void registerFilesystemIsolationServingProcess();

}
