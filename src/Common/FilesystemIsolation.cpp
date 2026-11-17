#include <Common/FilesystemIsolation.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <mutex>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace DB
{

namespace
{

/// The mode `isolateFilesystem` was called with, or -1 if it has not been called.
std::atomic<int> requested_mode{-1};
/// The mode in force, or -1 if `isolateFilesystem` has not returned.
std::atomic<int> applied_mode{-1};

std::mutex status_mutex;
std::optional<FilesystemIsolationStatus> saved_status;

/// The socket to the supervisor outside of the isolation, or -1 if the filesystem is not isolated.
std::atomic<int> supervisor_socket{-1};

std::optional<FilesystemIsolationMode> loadMode(const std::atomic<int> & mode_holder)
{
    const int mode = mode_holder.load(std::memory_order_relaxed);
    if (mode < 0)
        return std::nullopt;
    return static_cast<FilesystemIsolationMode>(mode);
}

FilesystemIsolationStatus rememberStatus(FilesystemIsolationMode mode, FilesystemIsolationStatus status)
{
    {
        std::lock_guard lock(status_mutex);
        saved_status = status;
    }
    applied_mode.store(static_cast<int>(status.isolated ? mode : FilesystemIsolationMode::Disabled), std::memory_order_relaxed);
    return status;
}

}

std::optional<FilesystemIsolationMode> getRequestedFilesystemIsolationMode()
{
    return loadMode(requested_mode);
}

std::optional<FilesystemIsolationMode> getAppliedFilesystemIsolationMode()
{
    return loadMode(applied_mode);
}

std::optional<FilesystemIsolationStatus> getFilesystemIsolationStatus()
{
    std::lock_guard lock(status_mutex);
    return saved_status;
}

}

#if defined(OS_LINUX)

#include <Common/Elf.h>
#include <Common/ErrnoException.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/getThreadId.h>
#include <base/scope_guard.h>

#include <Poco/Glob.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <set>

#include <dirent.h>
#include <elf.h>
#include <link.h>
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/capability.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <poll.h>
#include <sched.h>
#include <csignal>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace DB
{

namespace
{

/// Newer than the kernel headers of the sysroot. The same numbers on every architecture.
constexpr long syscall_pidfd_send_signal = 424; // NOLINT(google-runtime-int)
constexpr long syscall_pidfd_open = 434; // NOLINT(google-runtime-int)

/// A directory the new root is mounted on before `pivot_root` moves it to `/`. It is only needed to have a place
/// for the mount: `pivot_root` detaches the new root from it, so its original content stays visible inside the old
/// root, from which the paths are bind-mounted.
constexpr auto staging_directory = "/tmp";
/// Where `pivot_root` puts the old root, relative to the new one. It is unmounted at the end.
constexpr auto old_root = "/.old_root";

/// The devices every program expects. They are bind-mounted one by one; nothing else in `/dev` is visible.
constexpr std::array devices = {"null", "zero", "full", "random", "urandom"};

/// The capabilities dropped when the process keeps its capabilities, because it isolates itself with `CAP_SYS_ADMIN`
/// and does not create a user namespace. Each of them would let the process leave or undo the isolation, either
/// directly (`CAP_SYS_ADMIN` mounts and unmounts, `CAP_SYS_CHROOT`, `CAP_DAC_READ_SEARCH` opens files by handle on
/// any filesystem, `CAP_SYS_PTRACE` reaches another process's root through `/proc/<pid>/root`), by changing the
/// identity to the one of a process outside the isolation (`CAP_SETUID`, `CAP_SETGID`), or by reaching the hardware
/// or the kernel (`CAP_MKNOD` creates a device of a disk, `CAP_SYS_RAWIO`, `CAP_SYS_MODULE`, `CAP_BPF`, `CAP_PERFMON`),
/// or by giving the capabilities back (`CAP_SETPCAP`, `CAP_SETFCAP`). The others, such as `CAP_NET_BIND_SERVICE`,
/// `CAP_IPC_LOCK`, `CAP_SYS_NICE`, `CAP_NET_ADMIN`, are kept, because the server uses them.
constexpr std::array dangerous_capabilities = {
    CAP_SYS_ADMIN, CAP_SYS_CHROOT, CAP_DAC_READ_SEARCH, CAP_SYS_PTRACE, CAP_SETUID, CAP_SETGID, CAP_MKNOD,
    CAP_SYS_RAWIO, CAP_SYS_MODULE, CAP_SYS_BOOT, CAP_SETPCAP, CAP_SETFCAP, CAP_LINUX_IMMUTABLE, CAP_MAC_ADMIN,
    CAP_MAC_OVERRIDE, CAP_SYS_TIME, CAP_SYSLOG, CAP_AUDIT_CONTROL, CAP_SYS_PACCT, CAP_SYS_TTY_CONFIG,
    /// Newer than some kernel headers: `CAP_PERFMON`, `CAP_BPF`, `CAP_CHECKPOINT_RESTORE`.
    38, 39, 40};

constexpr std::array capability_names = {
    "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_DAC_READ_SEARCH", "CAP_FOWNER", "CAP_FSETID", "CAP_KILL", "CAP_SETGID",
    "CAP_SETUID", "CAP_SETPCAP", "CAP_LINUX_IMMUTABLE", "CAP_NET_BIND_SERVICE", "CAP_NET_BROADCAST", "CAP_NET_ADMIN",
    "CAP_NET_RAW", "CAP_IPC_LOCK", "CAP_IPC_OWNER", "CAP_SYS_MODULE", "CAP_SYS_RAWIO", "CAP_SYS_CHROOT",
    "CAP_SYS_PTRACE", "CAP_SYS_PACCT", "CAP_SYS_ADMIN", "CAP_SYS_BOOT", "CAP_SYS_NICE", "CAP_SYS_RESOURCE",
    "CAP_SYS_TIME", "CAP_SYS_TTY_CONFIG", "CAP_MKNOD", "CAP_LEASE", "CAP_AUDIT_WRITE", "CAP_AUDIT_CONTROL",
    "CAP_SETFCAP", "CAP_MAC_OVERRIDE", "CAP_MAC_ADMIN", "CAP_SYSLOG", "CAP_WAKE_ALARM", "CAP_BLOCK_SUSPEND",
    "CAP_AUDIT_READ", "CAP_PERFMON", "CAP_BPF", "CAP_CHECKPOINT_RESTORE"};

std::string capabilityName(unsigned capability)
{
    if (capability < capability_names.size())
        return capability_names[capability];
    return fmt::format("capability {}", capability);
}

struct Capabilities
{
    uint64_t effective = 0;
    uint64_t permitted = 0;
    uint64_t inheritable = 0;
};

Capabilities getCapabilities()
{
    __user_cap_header_struct header{_LINUX_CAPABILITY_VERSION_3, 0};
    __user_cap_data_struct data[2]{};
    if (0 != syscall(SYS_capget, &header, data))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot get the capabilities of the process");
    return {
        .effective = data[0].effective | (static_cast<uint64_t>(data[1].effective) << 32),
        .permitted = data[0].permitted | (static_cast<uint64_t>(data[1].permitted) << 32),
        .inheritable = data[0].inheritable | (static_cast<uint64_t>(data[1].inheritable) << 32)};
}

void setCapabilities(const Capabilities & capabilities)
{
    __user_cap_header_struct header{_LINUX_CAPABILITY_VERSION_3, 0};
    __user_cap_data_struct data[2]{};
    data[0].effective = static_cast<uint32_t>(capabilities.effective);
    data[1].effective = static_cast<uint32_t>(capabilities.effective >> 32);
    data[0].permitted = static_cast<uint32_t>(capabilities.permitted);
    data[1].permitted = static_cast<uint32_t>(capabilities.permitted >> 32);
    data[0].inheritable = static_cast<uint32_t>(capabilities.inheritable);
    data[1].inheritable = static_cast<uint32_t>(capabilities.inheritable >> 32);
    if (0 != syscall(SYS_capset, &header, data))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set the capabilities of the process");
}

bool hasCapability(uint64_t set, unsigned capability)
{
    return (set >> capability) & 1;
}

std::vector<std::string> capabilityNames(uint64_t set)
{
    std::vector<std::string> names;
    for (unsigned capability = 0; capability < 64; ++capability)
        if (hasCapability(set, capability))
            names.push_back(capabilityName(capability));
    return names;
}

/// Leaves only the capabilities in `keep` in every set, including the bounding set when that is allowed (it needs
/// `CAP_SETPCAP`). The bounding set matters only to a program the process executes, and no program can gain a
/// capability anyway: `PR_SET_NO_NEW_PRIVS` is set right after, and every mount is `nosuid`, which disables file
/// capabilities as well.
void restrictCapabilities(uint64_t keep)
{
    const Capabilities current = getCapabilities();

    if (hasCapability(current.effective, CAP_SETPCAP))
    {
        for (unsigned capability = 0; capability < 64; ++capability)
        {
            const int in_bounding_set = prctl(PR_CAPBSET_READ, capability, 0, 0, 0);
            if (in_bounding_set < 0)
                break; /// Past the last capability the kernel knows.
            if (in_bounding_set == 0 || hasCapability(keep, capability))
                continue;
            if (0 != prctl(PR_CAPBSET_DROP, capability, 0, 0, 0))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot drop {} from the capability bounding set", capabilityName(capability));
        }
    }

    /// The ambient set is cleared by the kernel of every capability that leaves the permitted or inheritable set.
    setCapabilities({
        .effective = current.effective & keep,
        .permitted = current.permitted & keep,
        .inheritable = current.inheritable & keep});

    if (0 != prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_NO_NEW_PRIVS");
}

void closeDescriptor(int fd)
{
    [[maybe_unused]] const int err = ::close(fd);
    chassert(!err || errno == EINTR);
}

std::string readFile(const std::string & path)
{
    ReadBufferFromFile in(path);
    std::string content;
    readStringUntilEOF(content, in);
    return content;
}

void writeFile(const std::string & path, std::string_view content)
{
    const int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open {}", path);
    SCOPE_EXIT({ closeDescriptor(fd); });
    const ssize_t written = ::write(fd, content.data(), content.size());
    if (written != static_cast<ssize_t>(content.size()))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot write '{}' to {}", content, path);
}

size_t countThreads()
{
    DIR * dir = ::opendir("/proc/self/task");
    if (!dir)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open /proc/self/task");
    SCOPE_EXIT({ ::closedir(dir); });

    size_t threads = 0;
    while (const dirent * entry = ::readdir(dir)) /// NOLINT(concurrency-mt-unsafe): the directory stream is not shared.
        if (entry->d_name[0] != '.')
            ++threads;
    return threads;
}

/// The threads of the process with their names, for a message.
std::vector<std::string> describeThreads()
{
    std::vector<std::string> threads;
    std::error_code error;
    for (const auto & entry : fs::directory_iterator("/proc/self/task", error))
    {
        std::string name;
        try
        {
            name = readFile(entry.path() / "comm");
        }
        catch (const Exception &) // NOLINT(bugprone-empty-catch): the thread is gone by now.
        {
        }
        if (name.ends_with('\n'))
            name.pop_back();
        threads.push_back(fmt::format("{} ({})", entry.path().filename().string(), name.empty() ? "exited" : name));
    }
    return threads;
}

/// Waits until the process has at most `max_threads` threads and returns their number, or the number of threads if
/// it has more after a second. A thread that was just joined is not gone yet: `pthread_join` returns when the kernel
/// clears the ID of the thread at the start of its exit, and the thread stays in `/proc/self/task` and in the thread
/// group, which `unshare` of a user namespace refuses, until it finishes exiting. There is no notification of that,
/// and with a single CPU, as in a container limited to one, it finishes only when this thread lets it run.
size_t waitForSingleThread(size_t max_threads)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    size_t threads = countThreads();
    while (threads > max_threads && std::chrono::steady_clock::now() < deadline)
    {
        ::sched_yield();
        threads = countThreads();
    }
    return threads;
}

/// An open directory, inherited or not, is a way out of the isolated filesystem: `openat` relative to it reaches
/// the original root, wherever the mounts are.
void checkNoOpenDirectories()
{
    DIR * dir = ::opendir("/proc/self/fd");
    if (!dir)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open /proc/self/fd");
    SCOPE_EXIT({ ::closedir(dir); });

    const int own_fd = ::dirfd(dir);
    while (const dirent * entry = ::readdir(dir)) /// NOLINT(concurrency-mt-unsafe): the directory stream is not shared.
    {
        if (entry->d_name[0] == '.')
            continue;
        const int fd = std::stoi(entry->d_name);
        if (fd == own_fd)
            continue;
        struct stat st{};
        if (0 != ::fstat(fd, &st))
            continue; /// Closed in the meantime.
        if (S_ISDIR(st.st_mode))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot isolate the filesystem, because the file descriptor {} refers to a directory, which would let the "
                "process reach outside the isolated filesystem. It was most likely inherited from the parent process",
                fd);
    }
}

/// Whether `path` is `ancestor` or is inside it.
bool isSameOrInside(std::string_view path, std::string_view ancestor)
{
    if (ancestor == "/")
        return true;
    return path.starts_with(ancestor) && (path.size() == ancestor.size() || path[ancestor.size()] == '/');
}

struct SymbolicLink
{
    std::string path;
    std::string target;
};

/// What the paths go through on the way to their real locations, which is recreated in the isolated filesystem, so
/// that the paths are found there in the same way as outside.
struct Traversal
{
    /// The symbolic links met on the way.
    std::vector<SymbolicLink> links;
    /// The directories without symbolic links left with `..`, as the target of `link` in `/srv/link/../data`, which
    /// is the parent of that target rather than `/srv`. They exist in the isolated filesystem, empty unless they are
    /// visible, for `..` to lead from them to the same place as outside.
    std::vector<std::string> directories;
};

/// Resolves the symbolic links in an absolute `path` in the current filesystem, as the kernel does: a `..` after a
/// link leads to the parent of its target. Appends every link met and every directory left with `..` on the way to
/// `traversal`. Returns the path without links, or nothing if the path does not exist.
std::optional<std::string> resolvePath(const std::string & path, Traversal & traversal)
{
    std::deque<std::string> components;
    for (const auto & component : fs::path(path).relative_path())
        components.push_back(component.string());

    std::string resolved = "/";
    size_t links_followed = 0;

    while (!components.empty())
    {
        const std::string component = std::move(components.front());
        components.pop_front();

        if (component.empty() || component == ".")
            continue;
        if (component == "..")
        {
            if (resolved != "/")
                traversal.directories.push_back(resolved);
            resolved = fs::path(resolved).parent_path().string();
            continue;
        }

        const std::string candidate = resolved == "/" ? "/" + component : resolved + "/" + component;
        struct stat st{};
        if (0 != ::lstat(candidate.c_str(), &st))
        {
            if (errno == ENOENT || errno == ENOTDIR)
                return std::nullopt;
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot stat {}", candidate);
        }

        if (!S_ISLNK(st.st_mode))
        {
            resolved = candidate;
            continue;
        }

        if (++links_followed > 40)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too many levels of symbolic links in {}", path);

        std::string target(PATH_MAX, '\0');
        const ssize_t length = ::readlink(candidate.c_str(), target.data(), target.size());
        if (length < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot read the symbolic link {}", candidate);
        target.resize(length);
        traversal.links.push_back({candidate, target});

        const fs::path target_path(target);
        std::vector<std::string> target_components;
        for (const auto & component_of_target : target_path.relative_path())
            target_components.push_back(component_of_target.string());
        components.insert(components.begin(), target_components.begin(), target_components.end());
        if (target_path.is_absolute())
            resolved = "/";
    }

    return resolved;
}

/// A path, without symbolic links, that is bind-mounted at the same location inside the isolated filesystem.
struct Mount
{
    std::string path;
    FilesystemIsolationPath::Access access;
    std::string source;
};

bool isOctalDigit(char c)
{
    return c >= '0' && c <= '7';
}

/// Decodes a path from `/proc/self/mountinfo`, where the kernel writes space, tab, newline and backslash as `\ooo`.
std::string decodeMountInfoPath(std::string_view encoded)
{
    std::string decoded;
    for (size_t i = 0; i < encoded.size(); ++i)
    {
        if (encoded[i] == '\\' && i + 3 < encoded.size() && isOctalDigit(encoded[i + 1]) && isOctalDigit(encoded[i + 2])
            && isOctalDigit(encoded[i + 3]))
        {
            decoded += static_cast<char>((encoded[i + 1] - '0') * 64 + (encoded[i + 2] - '0') * 8 + (encoded[i + 3] - '0'));
            i += 3;
        }
        else
            decoded += encoded[i];
    }
    return decoded;
}

struct MountInfo
{
    uint64_t id = 0;
    uint64_t parent_id = 0;
    /// `major:minor` of the filesystem.
    std::string device;
    /// The directory of the filesystem that is mounted, which differs from `/` for a bind mount.
    std::string root;
    std::string mount_point;
    std::string filesystem_type;
    /// The options of the filesystem, such as `upperdir=` of an overlay, without the unescaping of each option.
    std::string super_options;
};

/// Every mount in `/proc/self/mountinfo`.
std::vector<MountInfo> readMountInfo()
{
    std::vector<MountInfo> mounts;
    const std::string mount_info = readFile("/proc/self/mountinfo");
    size_t line_begin = 0;
    while (line_begin < mount_info.size())
    {
        size_t line_end = mount_info.find('\n', line_begin);
        if (line_end == std::string::npos)
            line_end = mount_info.size();
        const std::string_view line(mount_info.data() + line_begin, line_end - line_begin);
        line_begin = line_end + 1;

        /// The fields: the ID of the mount, the ID of its parent, the device, the root, the mount point, ...
        std::array<std::string_view, 5> fields;
        size_t field_begin = 0;
        bool complete = true;
        for (auto & field : fields)
        {
            const size_t field_end = line.find(' ', field_begin);
            if (field_end == std::string_view::npos)
            {
                complete = false;
                break;
            }
            field = line.substr(field_begin, field_end - field_begin);
            field_begin = field_end + 1;
        }
        if (!complete)
            continue;

        MountInfo mount;
        mount.id = parseFromString<uint64_t>(fields[0]);
        mount.parent_id = parseFromString<uint64_t>(fields[1]);
        mount.device = std::string(fields[2]);
        mount.root = decodeMountInfoPath(fields[3]);
        mount.mount_point = decodeMountInfoPath(fields[4]);
        /// The type of the filesystem is the field after the separator ` - `, which follows the optional fields.
        if (const size_t separator = line.find(" - ", field_begin); separator != std::string_view::npos)
        {
            const size_t type_begin = separator + 3;
            const size_t type_end = line.find(' ', type_begin);
            mount.filesystem_type = std::string(line.substr(type_begin, type_end - type_begin));
            /// Then the source and the options.
            if (type_end != std::string_view::npos)
            {
                if (const size_t source_end = line.find(' ', type_end + 1); source_end != std::string_view::npos)
                    mount.super_options = std::string(line.substr(source_end + 1));
            }
        }
        mounts.push_back(std::move(mount));
    }
    return mounts;
}

/// The ID of the mount that a lookup of `path` ends in, or nothing if the path does not exist or the process cannot
/// search a directory on the way, as for a mount covered by another one, or inside a directory that only root can
/// search. From `/proc/self/fdinfo` of a descriptor opened with `O_PATH`, which works with any kernel since 3.15, unlike
/// `STATX_MNT_ID`.
std::optional<uint64_t> getMountIdOfPath(const std::string & path)
{
    const int fd = ::open(path.c_str(), O_PATH | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
    {
        if (errno == ENOENT || errno == EACCES || errno == ENOTDIR)
            return {};
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open {}", path);
    }
    SCOPE_EXIT(closeDescriptor(fd));
    const std::string fd_info = readFile(fmt::format("/proc/self/fdinfo/{}", fd));
    constexpr std::string_view key = "mnt_id:";
    for (size_t line_begin = 0; line_begin < fd_info.size();)
    {
        size_t line_end = fd_info.find('\n', line_begin);
        if (line_end == std::string::npos)
            line_end = fd_info.size();
        const std::string_view line(fd_info.data() + line_begin, line_end - line_begin);
        line_begin = line_end + 1;
        if (line.starts_with(key))
        {
            std::string_view value = line.substr(key.size());
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
                value.remove_prefix(1);
            return parseFromString<uint64_t>(value);
        }
    }
    throw Exception(ErrorCodes::SYSTEM_ERROR, "There is no `mnt_id` in /proc/self/fdinfo/{}", fd);
}

/// The mounts that are visible at the mount point `path` and inside it, a mount before the mounts on it, except an
/// `autofs`: a lookup of it with nothing mounted on it would trigger the automount, which the service outside of this
/// mount namespace never completes here, and the filesystem mounted on it, if any, is in the result on its own. Not the mounts at the same paths that it covers, as the ones that a bind mount of a directory over itself
/// hides, which are not mount roots for a lookup of their paths. The visible mount at `path` is the one that no other
/// mount at `path` is mounted on: a mount on top of another one at the same mount point has it as its parent.
std::vector<MountInfo> getMountsInside(const std::string & path)
{
    const std::vector<MountInfo> all = readMountInfo();

    const MountInfo * top = nullptr;
    for (const auto & mount : all)
    {
        if (mount.mount_point != path)
            continue;
        const bool covered = std::any_of(all.begin(), all.end(), [&](const MountInfo & other)
            { return other.mount_point == path && other.parent_id == mount.id && other.id != mount.id; });
        if (!covered)
            top = &mount;
    }
    if (!top)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no mount at {} in the isolated filesystem", path);

    std::vector<MountInfo> connected = {*top};
    std::set<uint64_t> seen = {top->id};
    /// The children of the connected mounts, until there are no new ones.
    for (size_t checked = 0; checked < connected.size(); ++checked)
    {
        const uint64_t parent_id = connected[checked].id;
        for (const auto & mount : all)
            if (mount.parent_id == parent_id && mount.id != parent_id && isSameOrInside(mount.mount_point, path)
                && seen.insert(mount.id).second)
                connected.push_back(mount);
    }

    /// A connected mount can still be covered by another one in the same subtree, mounted on it or on a directory on
    /// the way to it, with the mounts on it: it is visible only if a lookup of its mount point ends in it.
    std::vector<MountInfo> result;
    for (auto & mount : connected)
        if (mount.filesystem_type != "autofs" && getMountIdOfPath(mount.mount_point) == mount.id)
            result.push_back(std::move(mount));
    return result;
}

/// The mount points at `path` and inside it.
std::vector<std::string> getMountPointsInside(const std::string & path)
{
    std::vector<std::string> mount_points;
    for (auto & mount : getMountsInside(path))
        mount_points.push_back(std::move(mount.mount_point));
    return mount_points;
}

/// Remounts the bind mount at `path` and every mount inside it with the flags `add`. A bind mount keeps the flags
/// of the mount it is made from, and the kernel refuses to clear `MS_RDONLY`, `MS_NOSUID`, `MS_NODEV` and
/// `MS_NOEXEC` of a mount that belongs to a more privileged user namespace, so these are carried over.
void addMountFlags(const std::string & path, unsigned long add) // NOLINT(google-runtime-int)
{
    for (const auto & mount_point : getMountPointsInside(path))
    {
        struct statvfs st{};
        if (0 != ::statvfs(mount_point.c_str(), &st))
        {
            /// A mount inside a directory the process cannot search, such as `/sys/kernel/debug/tracing`, which
            /// is unreachable for it anyway.
            if (errno == EACCES)
                continue;
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot get the mount flags of {}", mount_point);
        }

        unsigned long flags = add; // NOLINT(google-runtime-int)
        if (st.f_flag & ST_RDONLY)
            flags |= MS_RDONLY;
        if (st.f_flag & ST_NOSUID)
            flags |= MS_NOSUID;
        if (st.f_flag & ST_NODEV)
            flags |= MS_NODEV;
        if (st.f_flag & ST_NOEXEC)
            flags |= MS_NOEXEC;

        if (0 != ::mount(nullptr, mount_point.c_str(), nullptr, MS_REMOUNT | MS_BIND | flags, nullptr))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot remount {} with the flags {:#x}", mount_point, flags);
    }
}

/// Creates the directories of `path` that do not exist. Called only for the locations on the `tmpfs` of the new
/// root: a location inside a bind mount exists already, because it is a path without symbolic links that exists
/// outside.
void createDirectories(const std::string & path)
{
    std::error_code error;
    fs::create_directories(path, error);
    if (error)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot create the directory {} in the isolated filesystem: {}", path, error.message());
}

void createMountPoint(const std::string & path, bool is_directory)
{
    struct stat st{};
    if (0 == ::lstat(path.c_str(), &st))
        return;

    createDirectories(fs::path(path).parent_path());
    if (is_directory)
    {
        createDirectories(path);
        return;
    }

    const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
    if (fd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create the mount point {} in the isolated filesystem", path);
    closeDescriptor(fd);
}

/// Bind-mounts the path `path` of the old root at the same location in the new one.
void bindMount(const std::string & path, unsigned long add_flags) // NOLINT(google-runtime-int)
{
    const std::string source = old_root + path;
    struct stat st{};
    if (0 != ::stat(source.c_str(), &st))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot stat {}", path);

    createMountPoint(path, S_ISDIR(st.st_mode));
    if (0 != ::mount(source.c_str(), path.c_str(), nullptr, MS_BIND | MS_REC, nullptr))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot bind-mount {} into the isolated filesystem", path);
    if (add_flags)
        addMountFlags(path, add_flags);
}

/// The global controls in `/proc` that are writable by the user ID rather than by a capability, as `kernel.core_pattern`
/// with a pipe handler that the kernel runs as root in the initial namespaces, outside of the isolation, and
/// `/proc/sysrq-trigger`: they are made read-only, as in the containers of Docker. The server only reads them.
constexpr std::array proc_global_controls = {"/proc/sys", "/proc/sysrq-trigger", "/proc/bus", "/proc/fs", "/proc/irq"};

/// Bind-mounts `path` over itself, read-only, with every mount inside it. A path that does not exist is skipped.
/// Recursively, because a user namespace cannot bind a mount without its locked children, and the mounts inside are
/// remounted read-only as well, as a `binfmt_misc` at `/proc/sys/fs/binfmt_misc`, whose `register` changes how the
/// programs of the host are executed. An `autofs` is skipped: a lookup of it with nothing mounted on it would trigger
/// the automount, which the service outside of this mount namespace never completes here, and a filesystem that is
/// mounted on it is in the list on its own, after it, and is reached without triggering anything.
void remountReadOnly(const std::string & path)
{
    struct stat st{};
    if (0 != ::lstat(path.c_str(), &st))
        return;
    if (0 != ::mount(path.c_str(), path.c_str(), nullptr, MS_BIND | MS_REC, nullptr))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot bind-mount {} over itself", path);

    /// Every flag that a mount of a more privileged user namespace can lock is set, so none is cleared, and the flags
    /// of the access time are kept by a remount without them.
    for (const auto & mount : getMountsInside(path))
    {
        if (0 != ::mount(nullptr, mount.mount_point.c_str(), nullptr, MS_REMOUNT | MS_BIND | MS_RDONLY | MS_NOSUID | MS_NODEV | MS_NOEXEC, nullptr))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot remount {} read-only", mount.mount_point);
    }
}

void mountTmpfs(const std::string & path, unsigned long flags, const char * options) // NOLINT(google-runtime-int)
{
    if (0 != ::mount("tmpfs", path.c_str(), "tmpfs", flags, options))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot mount a tmpfs at {}", path);
}

/// A new instance of `devpts`, not shared with anything outside, for the pseudoterminals that the web terminal and
/// the SSH server open through `/dev/ptmx`. The pseudoterminals of the host stay invisible.
void mountDevpts(const std::string & path)
{
    if (0 != ::mount("devpts", path.c_str(), "devpts", MS_NOSUID | MS_NOEXEC, "newinstance,ptmxmode=0666,mode=0620"))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot mount a devpts at {}", path);
}

/// Creates a child process in new PID and mount namespaces, as `fork` does: it returns twice. The current process
/// stays in its namespaces. A raw `clone`, because `fork` has no flags for the namespaces. The child has a single
/// thread, like the current process.
pid_t cloneIntoPidAndMountNamespaces()
{
    const unsigned long flags = CLONE_NEWPID | CLONE_NEWNS | SIGCHLD; // NOLINT(google-runtime-int)
#if defined(__s390x__)
    /// The stack goes first on s390x.
    return static_cast<pid_t>(::syscall(SYS_clone, nullptr, flags, nullptr, nullptr, nullptr));
#else
    return static_cast<pid_t>(::syscall(SYS_clone, flags, nullptr, nullptr, nullptr, nullptr));
#endif
}

/// In a new user namespace, maps only the user and group of the process to themselves. An unprivileged process can
/// map only its own user and group, and only after it gives up `setgroups`.
void writeIdMaps(uid_t uid, gid_t gid)
{
    writeFile("/proc/self/setgroups", "deny");
    writeFile("/proc/self/uid_map", fmt::format("{0} {0} 1\n", uid));
    writeFile("/proc/self/gid_map", fmt::format("{0} {0} 1\n", gid));
}

/// In a new mount namespace, so that no mount propagates to the original one.
void makeMountsPrivate()
{
    if (0 != ::mount(nullptr, "/", nullptr, MS_REC | MS_PRIVATE, nullptr))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot make the mounts private");
}

/// Moves the current process into new user and mount namespaces, in which no mount propagates to the original one.
void enterUserAndMountNamespaces(uid_t uid, gid_t gid)
{
    if (::unshare(CLONE_NEWNS | CLONE_NEWUSER) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a user and a mount namespace");
    writeIdMaps(uid, gid);
    makeMountsPrivate();
}

/// A new instance of `proc` for the PID namespace of the calling process, which must be inside it. Only the processes
/// of that namespace are in it, so `/proc/<pid>/root`, `/proc/<pid>/cwd` and `/proc/<pid>/fd` lead nowhere outside.
void mountProc(const std::string & path)
{
    if (::mount("proc", path.c_str(), "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC, nullptr) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot mount a new proc at {}", path);
}

void installSupervisorSeccompFilter();

/// Checks in a child process whether this process would be allowed to create the namespaces and to mount the
/// filesystems in them. Returns the reason if not. The current process is left untouched: once it enters a user
/// namespace, it loses its capabilities of the initial one for good, even if nothing can be mounted there (as with
/// the restriction of unprivileged user namespaces by AppArmor in Ubuntu). Without a user namespace, also the new PID
/// namespace with a new `proc` in it, and what the supervisor outside of it needs.
std::string probeNamespaces(bool with_user_namespace, uid_t uid, gid_t gid)
{
    if (!with_user_namespace)
    {
#if !defined(__x86_64__) && !defined(__aarch64__)
        return "the seccomp filter of the supervisor outside of the isolation is implemented only for x86_64 and AArch64";
#endif
        /// The supervisor holds the process that serves by a pidfd.
        if (const int pidfd = static_cast<int>(::syscall(syscall_pidfd_open, ::getpid(), 0)); pidfd >= 0)
            closeDescriptor(pidfd);
        else
            return fmt::format("cannot create a pidfd, which needs Linux 5.3 or newer: {}", errnoToString(errno));
    }

    int pipe_fds[2];
    if (0 != ::pipe2(pipe_fds, O_CLOEXEC))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a pipe");

    const pid_t pid = with_user_namespace ? ::fork() : cloneIntoPidAndMountNamespaces();
    if (pid < 0)
    {
        const int error = errno;
        closeDescriptor(pipe_fds[0]);
        closeDescriptor(pipe_fds[1]);
        if (with_user_namespace)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot fork a process to check whether the filesystem can be isolated");
        return fmt::format("cannot create the PID and mount namespaces: {}", errnoToString(error));
    }

    if (pid == 0)
    {
        /// The current process has a single thread, so the child can do anything the parent could.
        closeDescriptor(pipe_fds[0]);
        int exit_code = 0;
        try
        {
            if (with_user_namespace)
                enterUserAndMountNamespaces(uid, gid);
            else
                makeMountsPrivate();
            mountTmpfs(staging_directory, MS_NOSUID | MS_NODEV, "mode=0755");
            const std::string pts = std::string(staging_directory) + "/pts";
            createDirectories(pts);
            mountDevpts(pts);
            if (with_user_namespace)
            {
                /// In its own mount namespace, so the original one is untouched.
                remountReadOnly("/proc/sys");
            }
            else
            {
                /// In the new PID namespace, whose init this child is.
                const std::string proc = std::string(staging_directory) + "/proc";
                createDirectories(proc);
                mountProc(proc);
                /// The controls of the new `proc`, as the server makes them read-only.
                remountReadOnly(proc + "/sys");
                /// The filter of the supervisor, which a policy outside may refuse. Last, because it allows no mount.
                if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0)
                    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_NO_NEW_PRIVS");
                installSupervisorSeccompFilter();
            }
        }
        catch (const Exception & e)
        {
            const std::string & message = e.message();
            [[maybe_unused]] ssize_t res = ::write(pipe_fds[1], message.data(), message.size());
            exit_code = 1;
        }
        catch (...)
        {
            const std::string message = getCurrentExceptionMessage(false);
            [[maybe_unused]] ssize_t res = ::write(pipe_fds[1], message.data(), message.size());
            exit_code = 1;
        }
        ::_exit(exit_code);
    }

    closeDescriptor(pipe_fds[1]);
    std::string message;
    char buf[1024];
    while (true)
    {
        const ssize_t res = ::read(pipe_fds[0], buf, sizeof(buf));
        if (res > 0)
            message.append(buf, res);
        else if (res == 0 || errno != EINTR)
            break;
    }
    closeDescriptor(pipe_fds[0]);

    int status = 0;
    while (::waitpid(pid, &status, 0) < 0)
        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot wait for the process that checks whether the filesystem can be isolated");

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
        return {};
    if (!message.empty())
        return message;
    return fmt::format("the process that checks whether the filesystem can be isolated terminated with the status {}", status);
}

/// The signals that the server handles and that are sent to it through its pid file: the requests to terminate, to
/// reopen the logs (`SIGHUP`), to print the stack traces (`SIGTSTP`), and `SIGABRT` for a crash report and a core dump.
constexpr std::array relayed_signals = {SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGTSTP, SIGABRT};

/// The signals that the supervisor and the init of the PID namespace handle. They are blocked in the supervisor before
/// it creates the init, so that none is lost or acts by default before they are read from a `signalfd`.
sigset_t getHandledSignals()
{
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGCHLD);
    for (const int sig : relayed_signals)
        sigaddset(&signals, sig);
    return signals;
}

bool isRelayedSignal(int sig)
{
    return std::find(relayed_signals.begin(), relayed_signals.end(), sig) != relayed_signals.end();
}

/// `CLICKHOUSE_WATCHDOG_NO_FORWARD=1`, as with systemd, which sends the signals to every process of the service.
bool isForwardingDisabled()
{
    const char * value = std::getenv("CLICKHOUSE_WATCHDOG_NO_FORWARD"); // NOLINT(concurrency-mt-unsafe)
    return value && std::string_view(value) == "1";
}

/// Writes a message to `stderr` without allocating, for the supervisor and the init.
void writeToStderr(std::string_view message)
{
    while (!message.empty())
    {
        const ssize_t res = ::write(STDERR_FILENO, message.data(), message.size());
        if (res < 0 && errno == EINTR)
            continue;
        if (res <= 0)
            return;
        message.remove_prefix(res);
    }
}

void setProcessName(const char * name)
{
    [[maybe_unused]] const int res = ::prctl(PR_SET_NAME, name, 0, 0, 0);
}

/// The pid file that the supervisor owns: created and locked before the namespaces, so that a server already running
/// stops the startup, and removed when the supervisor exits. The server inside the isolation does not see it, and its
/// directory must not be writable inside (see `checkPidFile`). Still, it is created and removed relative to a
/// descriptor of its directory, without following a symbolic link, and removed only if its name still refers to it.
struct SupervisorPidFile
{
    int directory_fd = -1;
    int fd = -1;
    std::string name;
    dev_t device = 0;
    ino_t inode = 0;

    void close()
    {
        if (fd >= 0)
            closeDescriptor(fd);
        if (directory_fd >= 0)
            closeDescriptor(directory_fd);
        fd = -1;
        directory_fd = -1;
    }

    void remove() const
    {
        struct stat st{};
        if (fd >= 0 && ::fstatat(directory_fd, name.c_str(), &st, AT_SYMLINK_NOFOLLOW) == 0 && st.st_dev == device
            && st.st_ino == inode)
            ::unlinkat(directory_fd, name.c_str(), 0);
    }
};

SupervisorPidFile createPidFile(const std::string & path, pid_t pid)
{
    SupervisorPidFile result;
    const fs::path normal = fs::absolute(path).lexically_normal();
    result.name = normal.filename().string();
    /// Without following a symbolic link on the way, which `checkPidFile` has refused.
    result.directory_fd = ::open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
    if (result.directory_fd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open /");
    try
    {
        for (const auto & component : normal.parent_path().relative_path())
        {
            if (component.empty() || component == ".")
                continue;
            const int next = ::openat(result.directory_fd, component.c_str(), O_PATH | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (next < 0)
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open the directory {} on the way to the pid file {}", component.string(), path);
            closeDescriptor(result.directory_fd);
            result.directory_fd = next;
        }
        /// Not blocking on a FIFO.
        result.fd = ::openat(result.directory_fd, result.name.c_str(), O_WRONLY | O_CREAT | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC, 0666);
        if (result.fd < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open the pid file {}", path);
        struct stat st{};
        if (::fstat(result.fd, &st) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot stat the pid file {}", path);
        if (!S_ISREG(st.st_mode) || st.st_nlink != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pid file {} is not a regular file with a single link", path);
        result.device = st.st_dev;
        result.inode = st.st_ino;
        if (::flock(result.fd, LOCK_EX | LOCK_NB) != 0)
        {
            if (errno == EWOULDBLOCK)
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot lock file {}. Another server instance in same directory is already running.", path);
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot lock the pid file {}", path);
        }
        if (::ftruncate(result.fd, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot truncate the pid file {}", path);
        const std::string content = std::to_string(pid);
        if (::write(result.fd, content.data(), content.size()) != static_cast<ssize_t>(content.size()))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot write the pid file {}", path);
    }
    catch (...)
    {
        result.close();
        throw;
    }
    return result;
}

/// Closes every descriptor that is not in `keep`.
void closeOtherDescriptors(std::initializer_list<int> keep)
{
    std::vector<int> to_close;
    {
        DIR * dir = ::opendir("/proc/self/fd");
        if (!dir)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open /proc/self/fd");
        SCOPE_EXIT({ ::closedir(dir); });
        const int own_fd = ::dirfd(dir);
        while (const dirent * entry = ::readdir(dir)) /// NOLINT(concurrency-mt-unsafe): the directory stream is not shared.
        {
            if (entry->d_name[0] == '.')
                continue;
            const int fd = std::stoi(entry->d_name);
            if (fd != own_fd && std::find(keep.begin(), keep.end(), fd) == keep.end())
                to_close.push_back(fd);
        }
    }
    for (const int fd : to_close)
        closeDescriptor(fd);
}

/// The system calls of the loop of the supervisor, and of what it calls: the C library and the allocator.
void installSupervisorSeccompFilter()
{
#if defined(__x86_64__) || defined(__aarch64__)
#    if defined(__x86_64__)
    constexpr UInt32 arch = AUDIT_ARCH_X86_64;
#    else
    constexpr UInt32 arch = AUDIT_ARCH_AARCH64;
#    endif
    const std::vector<UInt32> allowed = {
        __NR_read, __NR_write, __NR_close, __NR_ppoll, __NR_recvmsg, __NR_recvfrom, __NR_sendto, __NR_sendmsg,
        __NR_wait4, __NR_waitid, __NR_kill, __NR_tgkill, __NR_getpid, __NR_gettid, __NR_unlinkat, __NR_newfstatat, __NR_exit,
        __NR_exit_group, __NR_rt_sigprocmask, __NR_rt_sigaction, __NR_rt_sigreturn, __NR_restart_syscall,
        __NR_prlimit64, __NR_setrlimit, __NR_clock_gettime, __NR_gettimeofday, __NR_brk, __NR_mmap, __NR_munmap,
        __NR_mremap, __NR_madvise, __NR_futex, __NR_sched_yield, syscall_pidfd_open, syscall_pidfd_send_signal,
#    if defined(__NR_statx)
        __NR_statx,
#    endif
#    if defined(__x86_64__)
        __NR_poll,
#    endif
    };
    std::vector<sock_filter> program;
    program.push_back(BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)));
    program.push_back(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, arch, 1, 0));
    program.push_back(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS));
    program.push_back(BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)));
    for (size_t i = 0; i < allowed.size(); ++i)
        program.push_back(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, allowed[i], static_cast<UInt8>(allowed.size() - i), 0));
    program.push_back(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS));
    program.push_back(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));

    sock_fprog fprog{.len = static_cast<unsigned short>(program.size()), .filter = program.data()};
    if (::prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &fprog, 0, 0) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot install the seccomp filter of the supervisor");
#else
    /// `probeNamespaces` does not let the isolation start there.
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The seccomp filter of the supervisor is not implemented for this architecture");
#endif
}

/// A notification of the server that the supervisor relays to systemd: `READY=1`, `STOPPING=1`, or `STATUS=` with
/// printable text. Not `MAINPID=`: a PID inside the namespace means another process outside, and the supervisor is
/// the main process anyway.
bool isRelayedNotification(std::string_view message)
{
    if (message == "READY=1" || message == "STOPPING=1")
        return true;
    if (!message.starts_with("STATUS=") || message.size() > 256)
        return false;
    return std::all_of(message.begin(), message.end(), [](char c) { return c >= 0x20 && c < 0x7f; });
}

/// The messages of the server to the supervisor, besides the notifications: a request for its PID outside of the
/// namespace, and the registration of the process that serves, which gets the signals sent through the pid file. The
/// supervisor answers the request and every notification, which the server waits for.
constexpr std::string_view host_pid_request = "HOST_PID";
constexpr std::string_view host_pid_reply_prefix = "HOST_PID=";
constexpr std::string_view serving_registration = "SERVING";
constexpr std::string_view notify_reply_prefix = "NOTIFY=";

struct NotifySocket
{
    int fd = -1;
    sockaddr_un address{};
    socklen_t address_size = 0;
};

/// The socket of systemd from `NOTIFY_SOCKET`, as `systemdNotify` reads it, with a name starting with `@` for an
/// abstract socket.
NotifySocket openNotifySocket()
{
    NotifySocket result;
    const char * path = std::getenv("NOTIFY_SOCKET"); // NOLINT(concurrency-mt-unsafe)
    if (!path || (*path != '/' && *path != '@'))
        return result;
    const size_t length = std::strlen(path);
    if (length < 2 || length >= sizeof(result.address.sun_path))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Wrong UNIX path \"{}\" in NOTIFY_SOCKET env var", path);
    result.address.sun_family = AF_UNIX;
    std::memcpy(result.address.sun_path, path, length);
    result.address_size = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + length);
    if (*path == '@')
        result.address.sun_path[0] = '\0';
    else
        ++result.address_size; /// With the terminating zero.
    result.fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (result.fd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Can't create UNIX socket for systemd notify");
    return result;
}

/// Receives one message of the server, at most `max_size` bytes, with the PID of its sender outside of the namespace.
/// Returns the size, 0 for nothing or a message that is dropped, -1 when nothing can be received anymore.
ssize_t receiveFromServer(int fd, char * buf, size_t max_size, pid_t & sender)
{
    iovec iov{.iov_base = buf, .iov_len = max_size};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(ucred))];
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    const ssize_t size = ::recvmsg(fd, &msg, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
    if (size < 0)
        return (errno == EAGAIN || errno == EINTR) ? 0 : -1;

    sender = 0;
    for (cmsghdr * cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg))
    {
        if (cmsg->cmsg_level != SOL_SOCKET)
            continue;
        if (cmsg->cmsg_type == SCM_CREDENTIALS && cmsg->cmsg_len == CMSG_LEN(sizeof(ucred)))
        {
            ucred credentials{};
            std::memcpy(&credentials, CMSG_DATA(cmsg), sizeof(credentials));
            sender = credentials.pid;
        }
        else if (cmsg->cmsg_type == SCM_RIGHTS)
        {
            /// The space is only for the credentials, so the kernel does not pass descriptors, but if it did, they are
            /// not accepted.
            const size_t count = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
            for (size_t i = 0; i < count; ++i)
            {
                int passed = -1;
                std::memcpy(&passed, CMSG_DATA(cmsg) + i * sizeof(int), sizeof(int));
                closeDescriptor(passed);
            }
        }
    }
    if ((msg.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) || static_cast<size_t>(size) > max_size)
        return 0;
    return size;
}

/// Exits with the status of the server: with the same code, or by the same signal, without a core dump of the
/// supervisor, so that a service manager sees the same as without the isolation.
[[noreturn]] void exitWithStatus(int status)
{
    if (WIFEXITED(status))
        ::_exit(WEXITSTATUS(status));
    if (WIFSIGNALED(status))
    {
        const int sig = WTERMSIG(status);
        rlimit no_core{0, 0};
        ::setrlimit(RLIMIT_CORE, &no_core);
        ::signal(sig, SIG_DFL);
        sigset_t signals;
        sigemptyset(&signals);
        sigaddset(&signals, sig);
        ::sigprocmask(SIG_UNBLOCK, &signals, nullptr);
        ::kill(::getpid(), sig);
        /// A signal that does not terminate a process.
        ::_exit(128 + sig);
    }
    ::_exit(1);
}

/// The loop of the supervisor, the original process, which stays outside of the namespaces. `init_pid` is the init of
/// the PID namespace, `server_socket` the end of the socket pair to the server, `exit_status_fd` the pipe on which the
/// init writes the status of the server.
[[noreturn]] void runSupervisor(pid_t init_pid, int server_socket, int exit_status_fd, const SupervisorPidFile & pid_file)
{
    setProcessName("clickhouse-isol");
    /// A signal sent to the supervisor goes straight to the process that serves, as it went when the pid file had its
    /// PID. That process registers itself where the pid file used to be created, and a signal that arrives before is
    /// held until then. It is held by a pidfd, so a reused PID is never signalled, and its PID is the sender that the
    /// kernel attests. The processes inside are in a process group of their own, so a signal to the process group of
    /// the supervisor, such as `SIGINT` on Ctrl+C, reaches the server only from here, once: the server takes a second
    /// termination signal as an immediate termination. Not, with `CLICKHOUSE_WATCHDOG_NO_FORWARD=1`, a signal from the
    /// parent, the service manager, which sends the signals to every process of the service, as systemd does.
    const bool forwarding_disabled = isForwardingDisabled();
    const pid_t parent_pid = ::getppid();
    int serving_pidfd = -1;
    uint64_t pending_signals = 0;

    int signal_fd = -1;
    NotifySocket notify;
    try
    {
        notify = openNotifySocket();
        const sigset_t handled = getHandledSignals();
        signal_fd = ::signalfd(-1, &handled, SFD_CLOEXEC);
        if (signal_fd < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a signalfd");
        closeOtherDescriptors({STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO, server_socket, exit_status_fd, pid_file.directory_fd, pid_file.fd, signal_fd, notify.fd});
        /// It has nothing to do that needs a capability.
        restrictCapabilities(0);
        installSupervisorSeccompFilter();
    }
    catch (...)
    {
        writeToStderr(fmt::format("Cannot isolate the filesystem: cannot set up the supervisor: {}\n", getCurrentExceptionMessage(false)));
        ::kill(init_pid, SIGKILL);
        pid_file.remove();
        ::_exit(1);
    }

    auto reply = [&](std::string_view prefix, long value) // NOLINT(google-runtime-int)
    {
        char text[64];
        const int length = std::snprintf(text, sizeof(text), "%.*s%ld", static_cast<int>(prefix.size()), prefix.data(), value);
        if (length > 0)
            ::send(server_socket, text, length, MSG_DONTWAIT | MSG_NOSIGNAL);
    };

    /// Held until the next registration if no process serves, as after the server exited and before the watchdog
    /// restarts it (`CLICKHOUSE_WATCHDOG_RESTART=1`).
    auto deliver = [&](int sig)
    {
        if (serving_pidfd >= 0 && ::syscall(syscall_pidfd_send_signal, serving_pidfd, sig, nullptr, 0) == 0)
            return;
        if (serving_pidfd < 0 || errno == ESRCH)
        {
            if (serving_pidfd >= 0)
                closeDescriptor(serving_pidfd);
            serving_pidfd = -1;
            pending_signals |= uint64_t(1) << sig;
            return;
        }
        char text[128];
        const int length = std::snprintf(text, sizeof(text), "Cannot send the signal %d to the server: errno %d\n", sig, errno);
        if (length > 0)
            writeToStderr(std::string_view(text, std::min<size_t>(length, sizeof(text) - 1)));
    };

    char buf[512];
    pollfd fds[2] = {{.fd = signal_fd, .events = POLLIN, .revents = 0}, {.fd = server_socket, .events = POLLIN, .revents = 0}};
    while (true)
    {
        if (::poll(fds, 2, -1) < 0)
        {
            if (errno == EINTR)
                continue;
            ::kill(init_pid, SIGKILL);
            exitWithStatus(1 << 8);
        }

        if (fds[1].revents)
        {
            pid_t sender = 0;
            const ssize_t size = receiveFromServer(server_socket, buf, sizeof(buf), sender);
            /// Every process of the server has closed its end: stop polling it.
            if (size < 0 || (size == 0 && (fds[1].revents & (POLLHUP | POLLERR))))
                fds[1].fd = -1;
            std::string_view message(buf, size > 0 ? size : 0);
            if (message.ends_with('\n'))
                message.remove_suffix(1);
            if (message.empty() || sender <= 0)
            {
                /// Nothing, or not from a process.
            }
            else if (message == serving_registration)
            {
                const int pidfd = static_cast<int>(::syscall(syscall_pidfd_open, sender, 0));
                if (pidfd >= 0)
                {
                    if (serving_pidfd >= 0)
                        closeDescriptor(serving_pidfd);
                    serving_pidfd = pidfd;
                    const uint64_t to_deliver = pending_signals;
                    pending_signals = 0;
                    for (const int sig : relayed_signals)
                    {
                        if (to_deliver & (uint64_t(1) << sig))
                            deliver(sig);
                    }
                }
            }
            else if (message == host_pid_request)
                reply(host_pid_reply_prefix, sender);
            else if (message.starts_with("MAINPID="))
            {
                /// A PID inside the namespace means another process outside, and the supervisor is the main process.
                reply(notify_reply_prefix, 0);
            }
            else if (!isRelayedNotification(message))
                reply(notify_reply_prefix, EINVAL);
            else if (notify.fd < 0)
                reply(notify_reply_prefix, 0); /// Not started by systemd.
            else
            {
                buf[message.size()] = '\n';
                const ssize_t res = ::sendto(notify.fd, buf, message.size() + 1, MSG_NOSIGNAL, reinterpret_cast<const sockaddr *>(&notify.address), notify.address_size);
                reply(notify_reply_prefix, res < 0 ? errno : 0);
            }
        }

        if (fds[0].revents & POLLIN)
        {
            signalfd_siginfo info{};
            if (::read(signal_fd, &info, sizeof(info)) != sizeof(info))
                continue;
            const int sig = static_cast<int>(info.ssi_signo);
            if (sig == SIGCHLD)
            {
                int status = 0;
                const pid_t pid = ::waitpid(init_pid, &status, WNOHANG);
                if (pid != init_pid || !(WIFEXITED(status) || WIFSIGNALED(status)))
                    continue;
                /// The status of the server, if the init could report it.
                int server_status = 0;
                if (WIFEXITED(status) && WEXITSTATUS(status) == 0
                    && ::read(exit_status_fd, &server_status, sizeof(server_status)) == sizeof(server_status))
                    status = server_status;
                pid_file.remove();
                exitWithStatus(status);
            }
            if (!isRelayedSignal(sig) || (forwarding_disabled && static_cast<pid_t>(info.ssi_pid) == parent_pid))
                continue;
            deliver(sig);
        }
    }
}

/// The init of the PID namespace, the first process in it. It reaps the orphans and exits with the status of the
/// server; the signals for the server come to it from the supervisor directly. If the supervisor dies, so does the
/// init, and with it every process of the namespace. Returns in the server, its only child; does not return in the
/// init itself.
void runInit(int server_socket, int exit_status_fd, const sigset_t & original_signal_mask)
{
    pid_t server_pid = -1;
    try
    {
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_PDEATHSIG");
        /// Out of the process group of the supervisor, which relays the signals sent to the group.
        if (::setpgid(0, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a process group");
        /// The supervisor could have died before that: then its end of the socket pair is closed.
        pollfd fd{.fd = server_socket, .events = 0, .revents = 0};
        if (::poll(&fd, 1, 0) > 0 && (fd.revents & POLLHUP))
            ::_exit(1);

        /// `pivot_root` moves the root and the working directory of every process of the mount namespace that are at the
        /// original root to the new one, including the ones of the init, which keeps nothing of the original filesystem.
        if (::chdir("/") != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the directory to /");

        server_pid = ::fork();
        if (server_pid < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot fork the server");
        if (server_pid == 0)
        {
            closeDescriptor(exit_status_fd);
            updateCurrentThreadIdAfterFork();
            if (::sigprocmask(SIG_SETMASK, &original_signal_mask, nullptr) != 0)
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot restore the signal mask");
            /// The notifications go through the supervisor.
            ::unsetenv("NOTIFY_SOCKET"); // NOLINT(concurrency-mt-unsafe)
            supervisor_socket.store(server_socket, std::memory_order_relaxed);
            return;
        }

        setProcessName("clickhouse-init");
        closeDescriptor(server_socket);
        /// The init only waits and signals the processes of its own user. It is not dumpable, so the server cannot
        /// ptrace it or reach anything through its `/proc/1` entries.
        restrictCapabilities(0);
        if (::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_DUMPABLE");
    }
    catch (...)
    {
        writeToStderr(fmt::format("Cannot isolate the filesystem: {}\n", getCurrentExceptionMessage(false)));
        if (server_pid > 0)
            ::kill(server_pid, SIGKILL);
        ::_exit(1);
    }

    const sigset_t handled = getHandledSignals();
    const int signal_fd = ::signalfd(-1, &handled, SFD_CLOEXEC);
    if (signal_fd < 0)
    {
        writeToStderr("Cannot isolate the filesystem: cannot create a signalfd in the init\n");
        ::kill(server_pid, SIGKILL);
        ::_exit(1);
    }

    while (true)
    {
        signalfd_siginfo info{};
        if (::read(signal_fd, &info, sizeof(info)) != sizeof(info))
            continue;
        if (static_cast<int>(info.ssi_signo) == SIGCHLD)
        {
            /// The init of a PID namespace reaps every orphan in it.
            int status = 0;
            pid_t pid;
            while ((pid = ::waitpid(-1, &status, WNOHANG)) > 0)
            {
                if (pid == server_pid && (WIFEXITED(status) || WIFSIGNALED(status)))
                {
                    [[maybe_unused]] const ssize_t res = ::write(exit_status_fd, &status, sizeof(status));
                    ::_exit(0);
                }
            }
        }
    }
}

#if defined(__GLIBC__)
/// The services named in `/etc/nsswitch.conf`, such as `files`, `dns`, `systemd`, `sss`.
std::vector<std::string> getNameServices()
{
    /// The services the C library uses without `/etc/nsswitch.conf`.
    std::vector<std::string> services = {"files", "dns"};
    if (!fs::exists("/etc/nsswitch.conf"))
        return services;

    const std::string config = readFile("/etc/nsswitch.conf");
    size_t line_begin = 0;
    while (line_begin < config.size())
    {
        size_t line_end = config.find('\n', line_begin);
        if (line_end == std::string::npos)
            line_end = config.size();
        std::string_view line(config.data() + line_begin, line_end - line_begin);
        line_begin = line_end + 1;

        line = line.substr(0, line.find('#'));
        const size_t colon = line.find(':');
        if (colon == std::string_view::npos)
            continue;
        line.remove_prefix(colon + 1);

        /// A database is followed by its services, with the actions on the results in brackets between them,
        /// such as `hosts: files [NOTFOUND=return] dns`.
        bool in_action = false;
        size_t pos = 0;
        while (pos < line.size())
        {
            if (line[pos] == ' ' || line[pos] == '\t' || line[pos] == '\r')
            {
                ++pos;
                continue;
            }
            const size_t end = std::min(line.find_first_of(" \t\r", pos), line.size());
            const std::string_view token = line.substr(pos, end - pos);
            pos = end;
            if (token.starts_with('['))
                in_action = true;
            if (!in_action && std::find(services.begin(), services.end(), token) == services.end())
                services.emplace_back(token);
            if (token.ends_with(']'))
                in_action = false;
        }
    }
    return services;
}

/// The entries of the dynamic section of an ELF file that tell which shared libraries it needs and where they are.
struct DynamicEntries
{
    /// `DT_NEEDED`.
    std::vector<std::string> needed;
    /// `DT_RPATH` and `DT_RUNPATH`, the directories separated by `:`, with `$ORIGIN` expanded to the directory of the
    /// file. A directory with another substitution, as `$LIB` or `$PLATFORM`, is skipped. `DT_RPATH` is empty if the
    /// file has `DT_RUNPATH`, which the dynamic loader uses instead.
    std::vector<std::string> rpath;
    std::vector<std::string> runpath;
};

DynamicEntries getDynamicEntries(const std::string & path)
{
    const Elf elf(path);
    const auto dynamic = elf.findSectionByName(".dynamic");
    const auto strings = elf.findSectionByName(".dynstr");
    if (!dynamic || !strings)
        return {};

    const std::string origin = fs::path(path).parent_path().string();
    auto split_paths = [&](std::string_view value, std::vector<std::string> & directories)
    {
        size_t begin = 0;
        while (begin <= value.size())
        {
            size_t end = value.find(':', begin);
            if (end == std::string_view::npos)
                end = value.size();
            std::string directory(value.substr(begin, end - begin));
            begin = end + 1;
            for (const std::string_view variable : {"${ORIGIN}", "$ORIGIN"})
                for (size_t pos = directory.find(variable); pos != std::string::npos; pos = directory.find(variable, pos + origin.size()))
                    directory.replace(pos, variable.size(), origin);
            if (!directory.empty() && directory.front() == '/' && !directory.contains('$'))
                directories.push_back(std::move(directory));
        }
    };

    DynamicEntries entries;
    for (const char * entry_pos = dynamic->begin(); entry_pos + sizeof(ElfDyn) <= dynamic->end(); entry_pos += sizeof(ElfDyn))
    {
        ElfDyn entry{};
        memcpy(&entry, entry_pos, sizeof(entry));
        if (entry.tag == DynamicTableTag::Null)
            break;
        if (entry.tag != DynamicTableTag::NEEDED && entry.tag != DynamicTableTag::RPATH && entry.tag != DynamicTableTag::RUNPATH)
            continue;
        if (entry.val >= strings->size())
            throw Exception(ErrorCodes::SYSTEM_ERROR, "A string of the dynamic section of {} is outside of its string table", path);
        const char * value = strings->begin() + entry.val;
        const std::string_view string(value, strnlen(value, strings->end() - value));
        if (entry.tag == DynamicTableTag::NEEDED)
            entries.needed.emplace_back(string);
        else if (entry.tag == DynamicTableTag::RPATH)
            split_paths(string, entries.rpath);
        else
            split_paths(string, entries.runpath);
    }
    if (!entries.runpath.empty())
        entries.rpath.clear();
    return entries;
}

/// The directories where the dynamic loader finds the shared libraries: the directories of the mapped libraries,
/// such as the one of the C library, the directories of `/etc/ld.so.conf`, from which `ldconfig` builds
/// `/etc/ld.so.cache`, and the default directories.
std::vector<std::string> getLibraryDirectories(const std::vector<std::string> & mapped_files)
{
    std::vector<std::string> directories;
    auto add = [&](std::string directory)
    {
        if (std::find(directories.begin(), directories.end(), directory) == directories.end())
            directories.push_back(std::move(directory));
    };
    for (const auto & file : mapped_files)
        if (fs::path(file).filename().string().contains(".so"))
            add(fs::path(file).parent_path().string());

    /// With the files of its `include` directives.
    std::deque<std::string> configs = {"/etc/ld.so.conf"};
    std::set<std::string> seen_configs;
    while (!configs.empty())
    {
        const std::string config = std::move(configs.front());
        configs.pop_front();
        std::error_code error;
        if (!seen_configs.insert(config).second || !fs::is_regular_file(config, error))
            continue;
        std::ifstream in(config);
        std::string line;
        while (std::getline(in, line))
        {
            if (const size_t comment = line.find('#'); comment != std::string::npos)
                line.resize(comment);
            const size_t begin = line.find_first_not_of(" \t");
            if (begin == std::string::npos)
                continue;
            const size_t end = line.find_last_not_of(" \t\r");
            std::string value = line.substr(begin, end + 1 - begin);
            constexpr std::string_view include = "include";
            if (value.starts_with(include) && value.size() > include.size() && std::isspace(static_cast<unsigned char>(value[include.size()])))
            {
                std::string pattern = value.substr(value.find_first_not_of(" \t", include.size()));
                if (!pattern.starts_with('/'))
                    pattern = (fs::path(config).parent_path() / pattern).string();
                std::set<std::string> files;
                Poco::Glob::glob(pattern, files, 0);
                configs.insert(configs.end(), files.begin(), files.end());
            }
            else if (value.starts_with('/'))
                add(std::move(value));
        }
    }

    for (const auto * directory : {"/lib64", "/usr/lib64", "/lib", "/usr/lib"})
        add(directory);
    return directories;
}

/// The shared libraries with the names `names`, and the libraries they need, as the dynamic loader finds them. For a
/// library needed by an object without `DT_RUNPATH`: in the `DT_RPATH` of that object and of every object up the
/// chain of the objects that loaded it, to the executable, whether they have `DT_RUNPATH` or not. For a library needed
/// by an object with `DT_RUNPATH`: only in that, which applies to its direct dependencies. Then in `directories`.
/// `rpath` is the chain of `DT_RPATH` of the object that needs `names` and its loaders, `runpath` its `DT_RUNPATH`. A
/// library that is not found is unavailable to the dynamic loader as well.
std::vector<std::string> findLibraries(
    const std::vector<std::string> & names,
    const std::vector<std::string> & directories,
    const std::vector<std::string> & rpath = {},
    const std::vector<std::string> & runpath = {})
{
    struct Pending
    {
        std::string name;
        /// The `DT_RPATH` of the object that needs the library, then of the object that loaded that one, and so on.
        std::vector<std::string> rpath_chain;
        /// The `DT_RUNPATH` of the object that needs the library.
        std::vector<std::string> runpath;
    };
    std::deque<Pending> pending;
    for (const auto & name : names)
        pending.push_back({name, rpath, runpath});

    std::set<std::string> seen;
    std::vector<std::string> libraries;
    while (!pending.empty())
    {
        Pending library = std::move(pending.front());
        pending.pop_front();

        std::vector<std::string> candidates;
        /// A name with a slash is a path, as the dynamic loader takes it.
        if (library.name.contains('/'))
            candidates.push_back(library.name);
        else
        {
            if (library.runpath.empty())
                for (const auto & directory : library.rpath_chain)
                    candidates.push_back(directory + "/" + library.name);
            for (const auto & directory : library.runpath)
                candidates.push_back(directory + "/" + library.name);
            for (const auto & directory : directories)
                candidates.push_back(directory + "/" + library.name);
        }

        for (const auto & path : candidates)
        {
            std::error_code error;
            if (!fs::exists(path, error))
                continue;
            if (!seen.insert(path).second)
                break;
            libraries.push_back(path);
            DynamicEntries entries = getDynamicEntries(path);
            /// The chain for the libraries that this one needs: its own `DT_RPATH`, then the chain of its loaders, which
            /// is kept also when this one has `DT_RUNPATH` (and then no `DT_RPATH`), for the libraries further down.
            std::vector<std::string> rpath_chain = entries.rpath;
            rpath_chain.insert(rpath_chain.end(), library.rpath_chain.begin(), library.rpath_chain.end());
            for (auto & needed : entries.needed)
                pending.push_back({std::move(needed), rpath_chain, entries.runpath});
            break;
        }
    }
    return libraries;
}

/// The modules of the Name Service Switch for the services in `/etc/nsswitch.conf`, such as `libnss_files.so.2`
/// and `libnss_dns.so.2`, and the libraries they need. The C library loads them on the first lookup of a host, a
/// user or a group, which can happen after the isolation, so they are not among the mapped files. A service without a
/// module, such as one built into the C library, or one that is not installed, is unavailable to the C library as well.
std::vector<std::string> getNameServiceModules(const std::vector<std::string> & mapped_files)
{
    std::vector<std::string> names;
    for (const auto & service : getNameServices())
        names.push_back(fmt::format("libnss_{}.so.2", service));
    return findLibraries(names, getLibraryDirectories(mapped_files));
}

/// The dynamic loader and the shared libraries of an executable that the server starts after the isolation, such as
/// `clickhouse-odbc-bridge`, which can need libraries that the server itself does not map. Nothing for a file that is
/// not an ELF file, such as a script, or a statically linked one.
std::vector<std::string> getExecutableDependencies(const std::string & path, const std::vector<std::string> & mapped_files)
{
    {
        std::error_code error;
        if (!fs::is_regular_file(path, error))
            return {};
        char magic[4] = {};
        std::ifstream in(path, std::ios::binary);
        if (!in.read(magic, sizeof(magic)) || memcmp(magic, ELFMAG, SELFMAG) != 0)
            return {};
    }

    std::vector<std::string> dependencies;
    const Elf elf(path);
    if (const auto interpreter = elf.findSectionByName(".interp"))
        dependencies.emplace_back(interpreter->begin(), strnlen(interpreter->begin(), interpreter->size()));

    /// With the search paths of the executable, as for a library, but with `$ORIGIN` relative to the real location
    /// of the executable: the dynamic loader takes it from `/proc/self/exe`, with the links resolved, unlike for a
    /// library, whose `$ORIGIN` is the directory of the path it is opened by.
    const DynamicEntries entries = getDynamicEntries(fs::canonical(path).string());
    for (auto & library : findLibraries(entries.needed, getLibraryDirectories(mapped_files), entries.rpath, entries.runpath))
        dependencies.push_back(std::move(library));
    return dependencies;
}

/// The directories of the sockets of the services that the modules of the Name Service Switch for the services in
/// `/etc/nsswitch.conf` connect to, which `DT_NEEDED` does not tell: `avahi-daemon` for `nss-mdns`, `sssd` for
/// `sss`, `nslcd` for `ldap` of `nss-pam-ldapd`, and the services of the user database of `systemd` for `systemd`.
/// The ones of `nss-resolve` and of `nscd` are among the system paths of the server. The directories rather than the
/// sockets, because a service that restarts binds a new socket. A directory that does not exist is skipped.
std::vector<std::string> getNameServiceSocketDirectories()
{
    std::vector<std::string> directories;
    auto add = [&](const char * directory)
    {
        if (std::find(directories.begin(), directories.end(), directory) == directories.end())
            directories.emplace_back(directory);
    };
    for (const auto & service : getNameServices())
    {
        /// `mdns`, `mdns4`, `mdns6` and their variants `_minimal`.
        if (service.starts_with("mdns"))
            add("/run/avahi-daemon");
        else if (service == "sss")
            add("/var/lib/sss/pipes");
        else if (service == "ldap")
            add("/run/nslcd");
        else if (service == "systemd")
            add("/run/systemd/userdb");
    }
    return directories;
}
#endif

/// The files mapped into the process: its executable and, if it is not linked statically, the dynamic loader and
/// the libraries.
std::vector<std::string> getMappedFiles()
{
    std::vector<std::string> files;
    const std::string maps = readFile("/proc/self/maps");
    size_t line_begin = 0;
    while (line_begin < maps.size())
    {
        size_t line_end = maps.find('\n', line_begin);
        if (line_end == std::string::npos)
            line_end = maps.size();
        const std::string_view line(maps.data() + line_begin, line_end - line_begin);
        line_begin = line_end + 1;

        const size_t path_begin = line.find(" /");
        if (path_begin == std::string_view::npos)
            continue;
        const std::string_view path = line.substr(path_begin + 1);
        if (path.ends_with(" (deleted)"))
            continue;
        if (std::find(files.begin(), files.end(), path) == files.end())
            files.emplace_back(path);
    }
    return files;
}

/// The symbolic links next to the mapped libraries that point to them, such as `libc.so.6 -> libc-2.31.so`.
/// `/proc/self/maps` names the files with the links resolved, while the dynamic loader of a new process, such as
/// the ODBC bridge, looks the libraries up by these names.
std::vector<SymbolicLink> getLibraryAliases(const std::vector<std::string> & files)
{
    std::vector<SymbolicLink> aliases;
    std::vector<std::string> directories;
    for (const auto & file : files)
    {
        const std::string directory = fs::path(file).parent_path().string();
        if (std::find(directories.begin(), directories.end(), directory) != directories.end())
            continue;
        directories.push_back(directory);

        std::error_code error;
        fs::directory_iterator it(directory, error);
        if (error)
            continue;
        for (const auto & entry : it)
        {
            if (!entry.is_symlink(error) || error)
                continue;
            const fs::path target = fs::canonical(entry.path(), error);
            if (error || std::find(files.begin(), files.end(), target.string()) == files.end())
                continue;
            std::string link_target(PATH_MAX, '\0');
            const ssize_t length = ::readlink(entry.path().c_str(), link_target.data(), link_target.size());
            if (length < 0)
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot read the symbolic link {}", entry.path().string());
            link_target.resize(length);
            aliases.push_back({entry.path().string(), std::move(link_target)});
        }
    }
    return aliases;
}

/// The dynamic loader named in the executable, if it is not linked statically. `/proc/self/maps` names the file
/// with its symbolic links resolved, while a new process of the same executable, such as a bridge, looks it up
/// by this name.
std::optional<std::string> getInterpreter()
{
    std::optional<std::string> interpreter;
    dl_iterate_phdr([](dl_phdr_info * info, size_t, void * data)
    {
        for (size_t i = 0; i < info->dlpi_phnum; ++i)
        {
            if (info->dlpi_phdr[i].p_type == PT_INTERP)
            {
                *static_cast<std::optional<std::string> *>(data) = reinterpret_cast<const char *>(info->dlpi_addr + info->dlpi_phdr[i].p_vaddr);
                break;
            }
        }
        /// The first object is the executable.
        return 1;
    }, &interpreter);
    return interpreter;
}

std::string describeAccess(FilesystemIsolationPath::Access access)
{
    return access == FilesystemIsolationPath::Access::ReadWrite ? "read-write" : "read-only";
}

/// Resolves the paths, drops the missing ones, and the ones that a mount of a containing directory with the same
/// access makes visible already. The result is sorted so that a directory is mounted before the paths inside it.
std::vector<Mount> planMounts(const std::vector<FilesystemIsolationPath> & paths, Traversal & traversal)
{
    std::vector<Mount> resolved;
    for (const auto & path : paths)
    {
        if (!fs::path(path.path).is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The path {} ({}) to keep visible in the isolated filesystem is not absolute", path.path, path.source);

        /// The server would create it anyway, and there is no way to do it from inside. An existing path, which can be
        /// a file, such as a device for a log, is left as it is.
        if (std::error_code error; path.access == FilesystemIsolationPath::Access::ReadWrite && !fs::exists(path.path, error))
        {
            fs::create_directories(path.path, error);
            if (error)
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot create the directory {} ({}): {}", path.path, path.source, error.message());
        }

        auto real_path = resolvePath(path.path, traversal);
        if (!real_path)
            continue;
        /// Visible anyway, and links into them, such as `/etc/mtab`, are recreated.
        if (isSameOrInside(*real_path, "/proc") || isSameOrInside(*real_path, "/sys"))
            continue;
        /// The isolated filesystem has its own `/dev`, with the devices every program expects and a private `devpts`,
        /// which are made visible anyway. A path to one of them, such as `/dev/null` for a log, is not mounted again.
        if (*real_path == "/dev/ptmx" || isSameOrInside(*real_path, "/dev/pts")
            || std::any_of(devices.begin(), devices.end(), [&](const char * device) { return *real_path == fmt::format("/dev/{}", device); }))
            continue;
        /// The other devices of the host must stay invisible: a disk is readable and writable through its device
        /// without any capability, and the mounts in `/dev` keep their devices usable.
        if (*real_path == "/dev")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot isolate the filesystem, because the path {} ({}) is `/dev`, and keeping it visible would expose "
                "every device of the host. Add the devices that are needed one by one",
                path.path, path.source);
        if (*real_path == "/")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot isolate the filesystem, because the path {} ({}) is the root directory, and keeping it visible "
                "would make the isolation pointless",
                path.path, path.source);
        resolved.push_back({std::move(*real_path), path.access, path.source});
    }

    /// For equal paths, the read-write one goes first and wins: a read-write path is needed for writing, while a
    /// read-only one is only kept executable.
    std::sort(resolved.begin(), resolved.end(), [](const Mount & lhs, const Mount & rhs)
    {
        if (lhs.path != rhs.path)
            return lhs.path < rhs.path;
        return lhs.access == FilesystemIsolationPath::Access::ReadWrite && rhs.access != FilesystemIsolationPath::Access::ReadWrite;
    });

    std::vector<Mount> mounts;
    for (auto & mount : resolved)
    {
        /// The nearest containing mount decides what the path looks like already.
        const Mount * nearest = nullptr;
        for (auto it = mounts.rbegin(); it != mounts.rend(); ++it)
        {
            if (isSameOrInside(mount.path, it->path))
            {
                nearest = &*it;
                break;
            }
        }
        if (nearest && (nearest->path == mount.path || nearest->access == mount.access))
            continue;
        mounts.push_back(std::move(mount));
    }
    return mounts;
}

/// Where `path`, an existing path without symbolic links, is in its filesystem: the device of the filesystem and the
/// path inside it, which are the same under every name that bind mounts give it. Nothing if it does not exist.
std::optional<std::pair<std::string, std::string>> getFilesystemLocation(const std::string & path, const std::vector<MountInfo> & mount_info)
{
    const auto id = getMountIdOfPath(path);
    if (!id)
        return {};
    for (const auto & mount : mount_info)
    {
        if (mount.id != *id)
            continue;
        if (!isSameOrInside(path, mount.mount_point))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "The path {} is not inside its mount point {}", path, mount.mount_point);
        std::string location = (fs::path(mount.root) / fs::path(path).lexically_relative(mount.mount_point)).lexically_normal().string();
        while (location.size() > 1 && location.ends_with('/'))
            location.pop_back();
        return std::make_pair(mount.device, std::move(location));
    }
    throw Exception(ErrorCodes::SYSTEM_ERROR, "There is no mount with the ID {} of {} in /proc/self/mountinfo", *id, path);
}

/// The upper directory of an overlay from its options, where `\,` is a comma and `\\` a backslash.
std::optional<std::string> getOverlayUpperDirectory(const std::string & super_options)
{
    std::string option;
    for (size_t i = 0; i <= super_options.size(); ++i)
    {
        if (i == super_options.size() || super_options[i] == ',')
        {
            if (option.starts_with("upperdir="))
                return decodeMountInfoPath(option.substr(std::string_view("upperdir=").size()));
            option.clear();
            continue;
        }
        if (super_options[i] == '\\' && i + 1 < super_options.size() && (super_options[i + 1] == ',' || super_options[i + 1] == '\\'))
            ++i;
        option += super_options[i];
    }
    return {};
}

/// Why the pid file cannot be used with the isolation, or empty. The supervisor outside creates and removes it, so code
/// inside must not be able to change the pid file or a directory on the way to it, or it could make the supervisor write
/// or remove another file of the host at its next start. Such a directory is found by its path, by its location in its
/// filesystem, which a bind mount on the host can give another path, and through the upper directory of an overlay. A
/// storage can be visible under other names in ways that cannot be found, so no symbolic link is allowed on the way:
/// then code inside can only move directories within what it can write anyway.
std::string checkPidFile(const std::string & pid_file, const std::vector<Mount> & mounts)
{
    /// Component by component, as the kernel resolves it: `..` after a link would lead elsewhere than without the link.
    /// With no link allowed, `..` is the parent of the directory before it.
    std::vector<fs::path> on_the_way = {"/"};
    for (const auto & component : fs::absolute(pid_file).relative_path())
    {
        if (component.empty() || component == ".")
            continue;
        if (component == "..")
        {
            if (on_the_way.size() > 1)
                on_the_way.pop_back();
            continue;
        }
        const fs::path entry = on_the_way.back() / component;
        struct stat st{};
        if (::lstat(entry.c_str(), &st) == 0 && S_ISLNK(st.st_mode))
            return fmt::format(
                "the pid file {} has the symbolic link {} on its way, which code inside could change if the directory of the "
                "link is writable inside under any name, and make the supervisor outside, which creates and removes the pid "
                "file, write or remove another file. Use a path without symbolic links, such as one in `/run` rather than "
                "`/var/run`",
                pid_file, entry.string());
        on_the_way.push_back(entry);
    }

    const std::vector<MountInfo> mount_info = readMountInfo();
    std::vector<std::pair<const Mount *, std::pair<std::string, std::string>>> writable_locations;
    for (const auto & mount : mounts)
    {
        if (mount.access != FilesystemIsolationPath::Access::ReadWrite)
            continue;
        if (auto location = getFilesystemLocation(mount.path, mount_info))
            writable_locations.emplace_back(&mount, std::move(*location));
        /// What is written to an overlay goes to its upper directory.
        const auto id = getMountIdOfPath(mount.path);
        for (const auto & info : mount_info)
        {
            if (!id || info.id != *id || info.filesystem_type != "overlay")
                continue;
            const auto upper = getOverlayUpperDirectory(info.super_options);
            if (!upper)
                continue;
            const std::string in_overlay = (fs::path(info.root) / fs::path(mount.path).lexically_relative(info.mount_point)).lexically_normal().string();
            const std::string upper_path = (fs::path(*upper) / fs::path(in_overlay).relative_path()).lexically_normal().string();
            if (auto location = getFilesystemLocation(upper_path, mount_info))
                writable_locations.emplace_back(&mount, std::move(*location));
        }
    }

    for (const auto & entry : on_the_way)
    {
        const Mount * writable = nullptr;
        for (const auto & mount : mounts)
        {
            if (mount.access == FilesystemIsolationPath::Access::ReadWrite && isSameOrInside(entry.string(), mount.path))
                writable = &mount;
        }
        if (!writable)
        {
            if (const auto location = getFilesystemLocation(entry.string(), mount_info))
            {
                for (const auto & [mount, writable_location] : writable_locations)
                {
                    if (location->first == writable_location.first && isSameOrInside(location->second, writable_location.second))
                        writable = mount;
                }
            }
        }
        if (writable)
            return fmt::format(
                "the pid file {} is in a path writable inside the isolation ({}, {}), where code inside could replace a "
                "directory on its way, and make the supervisor outside, which creates and removes the pid file, write or "
                "remove another file. Move the pid file out of it",
                pid_file, writable->path, writable->source);
    }
    return {};
}

/// Why the paths cannot be made visible in a new PID namespace, or empty: a mount of `proc` inside one of them, as a
/// `/host/proc` of a monitoring agent, would come with its recursive bind mount, and show the processes outside of the
/// PID namespace, and their files through `/proc/<pid>/root`.
std::string findProcMountInside(const std::vector<Mount> & mounts)
{
    for (const auto & info : readMountInfo())
    {
        if (info.filesystem_type != "proc")
            continue;
        for (const auto & mount : mounts)
        {
            if (isSameOrInside(info.mount_point, mount.path))
                return fmt::format(
                    "the path {} ({}) has a mount of `proc` at {}, which would show the processes outside of the isolation, "
                    "and their files through `/proc/<pid>/root`",
                    mount.path, mount.source, info.mount_point);
        }
    }
    return {};
}

/// After the mounts, in the new PID namespace: every mount of `proc` is the new one or a part of it, which have its
/// device. A mount that appeared since `findProcMountInside` is not taken.
void checkNoOtherProcMount()
{
    const std::vector<MountInfo> mount_info = readMountInfo();
    std::string device;
    for (const auto & info : mount_info)
    {
        if (info.mount_point == "/proc" && info.filesystem_type == "proc")
            device = info.device;
    }
    for (const auto & info : mount_info)
    {
        if (info.filesystem_type == "proc" && info.device != device)
            throw Exception(
                ErrorCodes::SYSTEM_ERROR,
                "Cannot isolate the filesystem: there is a mount of `proc` at {} that is not the one of the new PID namespace",
                info.mount_point);
    }
}

/// Without a user namespace: the current process stays outside as the supervisor, creates the init of new PID and
/// mount namespaces, which forks the server, and never returns. Returns in the server only.
void startSupervisedServer(const std::string & pid_file)
{
    const pid_t supervisor_pid = ::getpid();
    int server_sockets[2];
    if (::socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, server_sockets) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a socket pair");
    const int pass_credentials = 1;
    int exit_status_pipe[2] = {-1, -1};
    SupervisorPidFile supervisor_pid_file;
    sigset_t original_signal_mask;
    try
    {
        if (::setsockopt(server_sockets[0], SOL_SOCKET, SO_PASSCRED, &pass_credentials, sizeof(pass_credentials)) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set SO_PASSCRED");
        if (::pipe2(exit_status_pipe, O_CLOEXEC) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a pipe");
        if (!pid_file.empty())
            supervisor_pid_file = createPidFile(pid_file, supervisor_pid);
    }
    catch (...)
    {
        for (const int fd : {server_sockets[0], server_sockets[1], exit_status_pipe[0], exit_status_pipe[1]})
        {
            if (fd >= 0)
                closeDescriptor(fd);
        }
        throw;
    }

    const sigset_t handled_signals = getHandledSignals();
    if (::sigprocmask(SIG_BLOCK, &handled_signals, &original_signal_mask) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot block the signals");

    const pid_t init_pid = cloneIntoPidAndMountNamespaces();
    if (init_pid < 0)
    {
        const int clone_error = errno;
        ::sigprocmask(SIG_SETMASK, &original_signal_mask, nullptr);
        for (const int fd : {server_sockets[0], server_sockets[1], exit_status_pipe[0], exit_status_pipe[1]})
            closeDescriptor(fd);
        supervisor_pid_file.remove();
        supervisor_pid_file.close();
        errno = clone_error;
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a process in new namespaces");
    }

    if (init_pid > 0)
    {
        closeDescriptor(server_sockets[1]);
        closeDescriptor(exit_status_pipe[1]);
        runSupervisor(init_pid, server_sockets[0], exit_status_pipe[0], supervisor_pid_file);
    }

    closeDescriptor(server_sockets[0]);
    closeDescriptor(exit_status_pipe[0]);
    /// In the init, which has a single thread and runs nothing else before: the directory of the pid file is a way
    /// out of the isolation.
    supervisor_pid_file.close();
    runInit(server_sockets[1], exit_status_pipe[1], original_signal_mask);

}

/// The capabilities the server keeps when it has `CAP_SYS_ADMIN`: all but `dangerous_capabilities`.
uint64_t getCapabilitiesToKeep()
{
    uint64_t keep = ~uint64_t(0);
    for (const auto capability : dangerous_capabilities)
        keep &= ~(uint64_t(1) << capability);
    return keep;
}

}

FilesystemIsolationStatus isolateFilesystem(
    FilesystemIsolationMode mode, const FilesystemIsolationPathsCollector & collect_paths, const std::string & pid_file)
{
    requested_mode.store(static_cast<int>(mode), std::memory_order_relaxed);

    if (mode == FilesystemIsolationMode::Disabled)
        return rememberStatus(mode, {});

#if defined(THREAD_SANITIZER)
    /// The runtime of the thread sanitizer starts its own background thread in the child of every `fork`, even if the
    /// parent did not have one, as in the process that `--daemon` forks, and there is no way to stop it then. It is
    /// not a thread of the server, and it does not prevent creating a child in new namespaces.
    constexpr size_t max_threads = 2;
#else
    constexpr size_t max_threads = 1;
#endif
    if (const size_t threads = waitForSingleThread(max_threads); threads > max_threads)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The filesystem must be isolated when the process has a single thread, but it has {}: {}", threads,
            fmt::join(describeThreads(), ", "));

    checkNoOpenDirectories();

    const uid_t uid = ::geteuid();
    const gid_t gid = ::getegid();
    const Capabilities capabilities = getCapabilities();
    const bool with_user_namespace = !hasCapability(capabilities.effective, CAP_SYS_ADMIN);

    if (const std::string reason = probeNamespaces(with_user_namespace, uid, gid); !reason.empty())
    {
        if (mode == FilesystemIsolationMode::Enforce)
            throw Exception(
                ErrorCodes::SYSTEM_ERROR,
                "Cannot isolate the filesystem: {}. Set the `filesystem_isolation` server setting to `best_effort` or `disabled` "
                "to run without the isolation",
                reason);

        FilesystemIsolationStatus not_isolated;
        not_isolated.not_isolated_reason = reason;
        return rememberStatus(mode, std::move(not_isolated));
    }

    /// Everything that needs the original filesystem is done before entering the namespaces.
    Traversal traversal;
    std::vector<FilesystemIsolationPath> all_paths
        = collect_paths ? collect_paths(/* with_supervisor= */ !with_user_namespace) : std::vector<FilesystemIsolationPath>{};
    const std::vector<std::string> mapped_files = getMappedFiles();
    for (const auto & file : mapped_files)
        all_paths.push_back({file, FilesystemIsolationPath::Access::ReadOnly, "mapped into the process"});
    /// The libraries of the programs that are started later, with the links that name them, as for the mapped files.
    /// Only with glibc: a build with musl is linked statically, as the programs it starts are.
    std::vector<std::string> executable_dependencies;
#if defined(__GLIBC__)
    for (const auto & path : std::vector<FilesystemIsolationPath>(all_paths))
        if (path.executable)
            for (auto & dependency : getExecutableDependencies(path.path, mapped_files))
            {
                all_paths.push_back({dependency, FilesystemIsolationPath::Access::ReadOnly, fmt::format("needed by {}", path.path)});
                executable_dependencies.push_back(std::move(dependency));
            }
#endif
    if (const auto interpreter = getInterpreter())
    {
        all_paths.push_back({*interpreter, FilesystemIsolationPath::Access::ReadOnly, "the dynamic loader of the executable"});
#if defined(__GLIBC__)
        all_paths.push_back({"/etc/ld.so.cache", FilesystemIsolationPath::Access::ReadOnly, "the cache of the dynamic loader"});
        for (auto & module : getNameServiceModules(mapped_files))
            all_paths.push_back({std::move(module), FilesystemIsolationPath::Access::ReadOnly, "a module of the Name Service Switch or a library it needs"});
        for (auto & directory : getNameServiceSocketDirectories())
            all_paths.push_back({std::move(directory), FilesystemIsolationPath::Access::ReadOnly, "the sockets of a service of the Name Service Switch"});
#endif
    }
    const std::vector<Mount> mounts = planMounts(all_paths, traversal);

    /// In a user namespace, the kernel refuses the server the files of the processes outside of it through any `proc`.
    if (!with_user_namespace)
    {
        if (const std::string reason = findProcMountInside(mounts); !reason.empty())
        {
            if (mode == FilesystemIsolationMode::Enforce)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot isolate the filesystem: {}", reason);
            FilesystemIsolationStatus not_isolated;
            not_isolated.not_isolated_reason = reason;
            return rememberStatus(mode, std::move(not_isolated));
        }
    }

    /// With a user namespace, there is no supervisor: the server creates the pid file itself, inside.
    if (!with_user_namespace && !pid_file.empty())
    {
        if (const std::string reason = checkPidFile(pid_file, mounts); !reason.empty())
        {
            if (mode == FilesystemIsolationMode::Enforce)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot isolate the filesystem: {}", reason);
            FilesystemIsolationStatus not_isolated;
            not_isolated.not_isolated_reason = reason;
            return rememberStatus(mode, std::move(not_isolated));
        }
    }
    for (auto & alias : getLibraryAliases(mapped_files))
        traversal.links.push_back(std::move(alias));
    for (auto & alias : getLibraryAliases(executable_dependencies))
        traversal.links.push_back(std::move(alias));

    std::error_code error;
    const std::string working_directory = fs::current_path(error).string();
    if (error)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot get the current working directory: {}", error.message());

    FilesystemIsolationStatus status;
    status.isolated = true;
    status.created_user_namespace = with_user_namespace;
    if (with_user_namespace)
        status.lost_capabilities = capabilityNames(capabilities.effective);

    /// From here on, in the server, any failure is final: the process cannot return to the original filesystem.
    if (with_user_namespace)
        enterUserAndMountNamespaces(uid, gid);
    else
    {
        status.created_pid_namespace = true;
        status.supervisor_pid = ::getpid();
        startSupervisedServer(pid_file);
        makeMountsPrivate();
    }

    mountTmpfs(staging_directory, MS_NOSUID | MS_NODEV, "mode=0755");
    const std::string put_old = std::string(staging_directory) + old_root;
    if (0 != ::mkdir(put_old.c_str(), 0700))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create {}", put_old);
    if (0 != ::syscall(SYS_pivot_root, staging_directory, put_old.c_str()))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the root filesystem");
    if (0 != ::chdir("/"))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the directory to the new root");

    /// It goes first, because the other mounts read `/proc/self/mountinfo`. Without a user namespace, a new `proc` of the
    /// new PID namespace, in which no process outside is visible: the server keeps the capabilities it uses, and a
    /// process outside of the same user and without capabilities would let it reach its filesystem through
    /// `/proc/<pid>/root`. In a user namespace, the kernel refuses that to the server for every process outside of it,
    /// and a new `proc` cannot be mounted without a new PID namespace, which a container that hides some of the files of
    /// `/proc` refuses as well: the original one is mounted, with its mounts inside, which a user namespace requires.
    if (with_user_namespace)
        bindMount("/proc", 0);
    else
    {
        createDirectories("/proc");
        mountProc("/proc");
    }
    for (const auto * control : proc_global_controls)
        remountReadOnly(control);
    status.visible_paths.push_back(with_user_namespace
        ? "/proc (with /proc/sys, /proc/sysrq-trigger, /proc/bus, /proc/fs, /proc/irq read-only)"
        : "/proc (a new instance for the PID namespace, with /proc/sys, /proc/sysrq-trigger, /proc/bus, /proc/fs, /proc/irq read-only)");

    /// A private `/tmp`, empty and not shared with anything outside. Mounted first, so that the paths inside `/tmp`
    /// are mounted on top of it.
    createDirectories("/tmp");
    mountTmpfs("/tmp", MS_NOSUID | MS_NODEV | MS_NOEXEC, "mode=1777");
    status.visible_paths.push_back("/tmp (read-write, a private empty tmpfs)");

    createDirectories("/dev");
    for (const auto * device : devices)
    {
        const std::string path = fmt::format("/dev/{}", device);
        struct stat st{};
        if (0 != ::stat((old_root + path).c_str(), &st))
            continue;
        bindMount(path, MS_NOSUID | MS_NOEXEC);
        status.visible_paths.push_back(path);
    }
    createDirectories("/dev/pts");
    mountDevpts("/dev/pts");
    status.visible_paths.push_back("/dev/pts (a private devpts)");
    for (const auto & [link, target] : std::initializer_list<std::pair<const char *, const char *>>{
             {"/dev/ptmx", "pts/ptmx"}, {"/dev/fd", "/proc/self/fd"}, {"/dev/stdin", "/proc/self/fd/0"}, {"/dev/stdout", "/proc/self/fd/1"}, {"/dev/stderr", "/proc/self/fd/2"}})
    {
        if (0 != ::symlink(target, link))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create the symbolic link {}", link);
    }

    bindMount("/sys", MS_RDONLY | MS_NOSUID | MS_NODEV | MS_NOEXEC);
    status.visible_paths.push_back("/sys (read-only)");

    for (const auto & mount : mounts)
    {
        const bool read_write = mount.access == FilesystemIsolationPath::Access::ReadWrite;
        unsigned long flags = read_write ? (MS_NOSUID | MS_NOEXEC) : (MS_RDONLY | MS_NOSUID); // NOLINT(google-runtime-int)
        /// A device stays usable only if it is explicitly listed.
        if (!isSameOrInside(mount.path, "/dev"))
            flags |= MS_NODEV;
        bindMount(mount.path, flags);
        status.visible_paths.push_back(fmt::format("{} ({}, {})", mount.path, describeAccess(mount.access), mount.source));
    }

    /// A link inside a bind mount exists there already. One in `/proc` or `/sys`, as `/proc/self` on the way of
    /// `/dev/stdout`, is what these filesystems show, and the PIDs in the new `proc` differ from the original ones.
    for (const auto & link : traversal.links)
    {
        if (isSameOrInside(link.path, "/proc") || isSameOrInside(link.path, "/sys"))
            continue;
        struct stat st{};
        if (0 == ::lstat(link.path.c_str(), &st))
            continue;
        createDirectories(fs::path(link.path).parent_path());
        if (0 != ::symlink(link.target.c_str(), link.path.c_str()))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create the symbolic link {} in the isolated filesystem", link.path);
    }

    /// A directory inside a bind mount exists there already.
    for (const auto & directory : traversal.directories)
    {
        if (isSameOrInside(directory, "/proc") || isSameOrInside(directory, "/sys"))
            continue;
        struct stat st{};
        if (0 != ::lstat(directory.c_str(), &st))
            createDirectories(directory);
    }

    /// Relative paths in the configuration keep their meaning: the working directory exists in the new root, empty
    /// unless it is visible. The process must leave the old one anyway, because it is a way out.
    if (!working_directory.empty())
        createDirectories(working_directory);

    if (0 != ::umount2(old_root, MNT_DETACH))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot unmount the original root filesystem");
    if (!with_user_namespace)
        checkNoOtherProcMount();
    if (0 != ::rmdir(old_root))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot remove {}", old_root);

    if (0 != ::mount(nullptr, "/", nullptr, MS_REMOUNT | MS_BIND | MS_RDONLY | MS_NOSUID | MS_NODEV, nullptr))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot make the root of the isolated filesystem read-only");

    if (0 != ::chdir(working_directory.empty() ? "/" : working_directory.c_str()))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the directory to {}", working_directory);

    if (with_user_namespace)
    {
        /// The capabilities in the new user namespace are what allowed the mounts, and nothing else needs them.
        restrictCapabilities(0);
    }
    else
        restrictCapabilities(getCapabilitiesToKeep());

    return rememberStatus(mode, std::move(status));
}

namespace
{

std::mutex supervisor_request_mutex;

/// Sends `request` to the supervisor and returns the rest of its answer after `reply_prefix`, or nothing if the
/// supervisor does not answer within 10 seconds. An answer to an earlier request that timed out is skipped.
std::optional<std::string> requestFromSupervisor(int fd, std::string_view request, std::string_view reply_prefix)
{
    std::lock_guard lock(supervisor_request_mutex);
    if (::send(fd, request.data(), request.size(), MSG_NOSIGNAL) < 0)
        return std::nullopt;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (true)
    {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (left <= 0)
            return std::nullopt;
        pollfd poll_fd{.fd = fd, .events = POLLIN, .revents = 0};
        const int res = ::poll(&poll_fd, 1, static_cast<int>(left));
        if (res < 0 && errno == EINTR)
            continue;
        if (res <= 0)
            return std::nullopt;
        char buf[64];
        const ssize_t size = ::recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (size < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        if (size <= 0)
            return std::nullopt;
        const std::string_view reply(buf, size);
        if (reply.starts_with(reply_prefix))
            return std::string(reply.substr(reply_prefix.size()));
    }
}

std::optional<int> parseNumber(std::string_view text)
{
    if (text.empty() || text.size() > 9)
        return std::nullopt;
    int value = 0;
    for (const char c : text)
    {
        if (c < '0' || c > '9')
            return std::nullopt;
        value = value * 10 + (c - '0');
    }
    return value;
}

}

bool notifyFilesystemIsolationSupervisor(std::string_view message)
{
    const int fd = supervisor_socket.load(std::memory_order_relaxed);
    if (fd < 0)
        return false;
    const auto reply = requestFromSupervisor(fd, message, notify_reply_prefix);
    if (!reply)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Failed to notify systemd: the supervisor of the filesystem isolation did not answer");
    const auto error = parseNumber(*reply);
    if (!error)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Failed to notify systemd: unexpected answer of the supervisor of the filesystem isolation");
    if (*error != 0)
    {
        errno = *error;
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to notify systemd, sendto returned error");
    }
    return true;
}

std::optional<int> getFilesystemIsolationHostPid()
{
    const int fd = supervisor_socket.load(std::memory_order_relaxed);
    if (fd < 0)
        return std::nullopt;
    const auto reply = requestFromSupervisor(fd, host_pid_request, host_pid_reply_prefix);
    if (!reply)
        return std::nullopt;
    return parseNumber(*reply);
}

void registerFilesystemIsolationServingProcess()
{
    const int fd = supervisor_socket.load(std::memory_order_relaxed);
    if (fd < 0)
        return;
    if (::send(fd, serving_registration.data(), serving_registration.size(), MSG_NOSIGNAL) < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot register with the supervisor of the filesystem isolation");
}

}

#else

namespace DB
{

FilesystemIsolationStatus isolateFilesystem(FilesystemIsolationMode mode, const FilesystemIsolationPathsCollector &, const std::string &)
{
    requested_mode.store(static_cast<int>(mode), std::memory_order_relaxed);

    FilesystemIsolationStatus status;
    if (mode == FilesystemIsolationMode::Enforce)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The filesystem isolation is implemented only on Linux");
    if (mode == FilesystemIsolationMode::BestEffort)
        status.not_isolated_reason = "the filesystem isolation is implemented only on Linux";
    return rememberStatus(mode, std::move(status));
}

bool notifyFilesystemIsolationSupervisor(std::string_view)
{
    return false;
}

std::optional<int> getFilesystemIsolationHostPid()
{
    return std::nullopt;
}

void registerFilesystemIsolationServingProcess()
{
}

}

#endif
