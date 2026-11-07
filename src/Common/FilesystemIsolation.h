#pragma once

#include <cstdint>
#include <optional>
#include <string>
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
};

struct FilesystemIsolationStatus
{
    /// Whether the filesystem is isolated.
    bool isolated = false;
    /// Whether a user namespace had to be created, because the process did not have `CAP_SYS_ADMIN`.
    bool created_user_namespace = false;
    /// Why the filesystem is not isolated although the mode asked for it. Empty if it is isolated or if the mode
    /// is `Disabled`.
    std::string not_isolated_reason;
    /// The capabilities of the initial user namespace that the process held before the isolation and does not
    /// have anymore, because the isolation had to create a user namespace.
    std::vector<std::string> lost_capabilities;
    /// The paths that are visible inside the isolated filesystem, with their access, for the log.
    std::vector<std::string> visible_paths;
};

/// Moves the current process into a new mount namespace whose root is an empty read-only `tmpfs`, with only
/// `paths` (and a few things every process needs: `/proc`, `/sys`, a handful of devices in `/dev`, a private
/// `/tmp`, the files mapped into the process, which include its executable) bind-mounted into it, and removes
/// the access to the original root. Then drops the capabilities that would let the process undo it.
///
/// If the process has `CAP_SYS_ADMIN`, only a mount namespace is created, and the process keeps the capabilities
/// it uses (such as `CAP_NET_BIND_SERVICE`, `CAP_IPC_LOCK`, `CAP_SYS_NICE`, `CAP_NET_ADMIN`). Otherwise a user
/// namespace is created as well, mapping only the current user and group to themselves, and the process loses
/// all its capabilities of the initial user namespace.
///
/// Must be called when the process has exactly one thread: a mount namespace belongs to a thread, and a
/// multithreaded process cannot enter a new user namespace. Throws a `LOGICAL_ERROR` exception otherwise.
///
/// In the `BestEffort` mode, if the process is not allowed to create the namespaces or to mount a filesystem in
/// them (which is checked in a child process first, so that the current process is not left in a half-made
/// namespace), returns a status with `isolated = false` and the reason. Any later failure, for example a path that
/// cannot be mounted, throws in both modes, because the process cannot go back to the original filesystem.
FilesystemIsolationStatus isolateFilesystem(FilesystemIsolationMode mode, const std::vector<FilesystemIsolationPath> & paths);

/// The mode `isolateFilesystem` was called with, or nothing if it has not been called.
std::optional<FilesystemIsolationMode> getRequestedFilesystemIsolationMode();

/// The mode in force: the requested one if the filesystem is isolated, `Disabled` otherwise. Nothing if
/// `isolateFilesystem` has not been called.
std::optional<FilesystemIsolationMode> getAppliedFilesystemIsolationMode();

/// The status returned by `isolateFilesystem`, or nothing if it has not been called.
std::optional<FilesystemIsolationStatus> getFilesystemIsolationStatus();

}
