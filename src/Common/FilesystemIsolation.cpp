#include <Common/FilesystemIsolation.h>
#include <Common/Exception.h>

#include <atomic>
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

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <deque>
#include <filesystem>

#include <dirent.h>
#include <elf.h>
#include <link.h>
#include <fcntl.h>
#include <linux/capability.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace DB
{

namespace
{

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
    while (const dirent * entry = ::readdir(dir))
        if (entry->d_name[0] != '.')
            ++threads;
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
    while (const dirent * entry = ::readdir(dir))
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

/// Resolves the symbolic links in an absolute `path` in the current filesystem, appending every link met on the
/// way to `links`. Returns the path without links, or nothing if the path does not exist.
std::optional<std::string> resolvePath(const std::string & path, std::vector<SymbolicLink> & links)
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
        links.push_back({candidate, target});

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

/// The mount points at `path` and inside it.
std::vector<std::string> getMountPointsInside(const std::string & path)
{
    std::vector<std::string> mount_points;
    const std::string mount_info = readFile("/proc/self/mountinfo");
    size_t line_begin = 0;
    while (line_begin < mount_info.size())
    {
        size_t line_end = mount_info.find('\n', line_begin);
        if (line_end == std::string::npos)
            line_end = mount_info.size();
        const std::string_view line(mount_info.data() + line_begin, line_end - line_begin);
        line_begin = line_end + 1;

        /// The fifth field is the mount point.
        size_t field_begin = 0;
        for (size_t field = 0; field < 4 && field_begin != std::string_view::npos; ++field)
        {
            field_begin = line.find(' ', field_begin);
            if (field_begin != std::string_view::npos)
                ++field_begin;
        }
        if (field_begin == std::string_view::npos)
            continue;
        const size_t field_end = line.find(' ', field_begin);
        const std::string mount_point = decodeMountInfoPath(line.substr(field_begin, field_end - field_begin));
        if (isSameOrInside(mount_point, path))
            mount_points.push_back(mount_point);
    }
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

void mountTmpfs(const std::string & path, unsigned long flags, const char * options) // NOLINT(google-runtime-int)
{
    if (0 != ::mount("tmpfs", path.c_str(), "tmpfs", flags, options))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot mount a tmpfs at {}", path);
}

/// Enters a new mount namespace (and a new user namespace, if `with_user_namespace`) in which no mount propagates
/// to the original one.
void enterNamespaces(bool with_user_namespace, uid_t uid, gid_t gid)
{
    if (0 != ::unshare(CLONE_NEWNS | (with_user_namespace ? CLONE_NEWUSER : 0)))
        throw ErrnoException(
            ErrorCodes::SYSTEM_ERROR, "Cannot create a {}", with_user_namespace ? "user and a mount namespace" : "mount namespace");

    if (with_user_namespace)
    {
        /// An unprivileged process can map only its own user and group, and only after it gives up `setgroups`.
        writeFile("/proc/self/setgroups", "deny");
        writeFile("/proc/self/uid_map", fmt::format("{0} {0} 1\n", uid));
        writeFile("/proc/self/gid_map", fmt::format("{0} {0} 1\n", gid));
    }

    if (0 != ::mount(nullptr, "/", nullptr, MS_REC | MS_PRIVATE, nullptr))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot make the mounts private");
}

/// Checks in a child process whether this process would be allowed to create the namespaces and to mount a
/// filesystem in them. Returns the reason if not. The current process is left untouched: once it enters a user
/// namespace, it loses its capabilities of the initial one for good, even if nothing can be mounted there (as with
/// the restriction of unprivileged user namespaces by AppArmor in Ubuntu).
std::string probeNamespaces(bool with_user_namespace, uid_t uid, gid_t gid)
{
    int pipe_fds[2];
    if (0 != ::pipe2(pipe_fds, O_CLOEXEC))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a pipe");

    const pid_t pid = ::fork();
    if (pid < 0)
    {
        closeDescriptor(pipe_fds[0]);
        closeDescriptor(pipe_fds[1]);
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot fork a process to check whether the filesystem can be isolated");
    }

    if (pid == 0)
    {
        /// The current process has a single thread, so the child can do anything the parent could.
        closeDescriptor(pipe_fds[0]);
        int exit_code = 0;
        try
        {
            enterNamespaces(with_user_namespace, uid, gid);
            mountTmpfs(staging_directory, MS_NOSUID | MS_NODEV, "mode=0755");
        }
        catch (const Exception & e)
        {
            const std::string message = e.message();
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
std::vector<Mount> planMounts(const std::vector<FilesystemIsolationPath> & paths, std::vector<SymbolicLink> & links)
{
    std::vector<Mount> resolved;
    for (const auto & path : paths)
    {
        if (!fs::path(path.path).is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The path {} ({}) to keep visible in the isolated filesystem is not absolute", path.path, path.source);

        if (path.access == FilesystemIsolationPath::Access::ReadWrite)
        {
            /// The server would create it anyway, and there is no way to do it from inside.
            std::error_code error;
            fs::create_directories(path.path, error);
            if (error)
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot create the directory {} ({}): {}", path.path, path.source, error.message());
        }

        auto real_path = resolvePath(path.path, links);
        if (!real_path)
            continue;
        /// Visible anyway, and links into them, such as `/etc/mtab`, are recreated.
        if (isSameOrInside(*real_path, "/proc") || isSameOrInside(*real_path, "/sys"))
            continue;
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

}

FilesystemIsolationStatus isolateFilesystem(FilesystemIsolationMode mode, const std::vector<FilesystemIsolationPath> & paths)
{
    requested_mode.store(static_cast<int>(mode), std::memory_order_relaxed);

    if (mode == FilesystemIsolationMode::Disabled)
        return rememberStatus(mode, {});

    if (const size_t threads = countThreads(); threads != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The filesystem must be isolated when the process has a single thread, but it has {}", threads);

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

        FilesystemIsolationStatus status;
        status.not_isolated_reason = reason;
        return rememberStatus(mode, std::move(status));
    }

    /// Everything that needs the original filesystem is done before entering the namespaces.
    std::vector<SymbolicLink> links;
    std::vector<FilesystemIsolationPath> all_paths = paths;
    for (const auto & file : getMappedFiles())
        all_paths.push_back({file, FilesystemIsolationPath::Access::ReadOnly, "mapped into the process"});
    if (const auto interpreter = getInterpreter())
        all_paths.push_back({*interpreter, FilesystemIsolationPath::Access::ReadOnly, "the dynamic loader of the executable"});
    const std::vector<Mount> mounts = planMounts(all_paths, links);

    std::error_code error;
    const std::string working_directory = fs::current_path(error).string();
    if (error)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot get the current working directory: {}", error.message());

    FilesystemIsolationStatus status;
    status.isolated = true;
    status.created_user_namespace = with_user_namespace;
    if (with_user_namespace)
        status.lost_capabilities = capabilityNames(capabilities.effective);

    /// From here on, any failure is final: the process cannot return to the original filesystem.
    enterNamespaces(with_user_namespace, uid, gid);

    mountTmpfs(staging_directory, MS_NOSUID | MS_NODEV, "mode=0755");
    const std::string put_old = std::string(staging_directory) + old_root;
    if (0 != ::mkdir(put_old.c_str(), 0700))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create {}", put_old);
    if (0 != ::syscall(SYS_pivot_root, staging_directory, put_old.c_str()))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the root filesystem");
    if (0 != ::chdir("/"))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the directory to the new root");

    /// A new `proc` cannot be mounted without a new PID namespace, so the original one is mounted, with its mounts
    /// inside, which a user namespace requires. It goes first, because the other mounts read `/proc/self/mountinfo`.
    bindMount("/proc", 0);
    status.visible_paths.push_back("/proc");

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
    for (const auto & [link, target] : std::initializer_list<std::pair<const char *, const char *>>{
             {"/dev/fd", "/proc/self/fd"}, {"/dev/stdin", "/proc/self/fd/0"}, {"/dev/stdout", "/proc/self/fd/1"}, {"/dev/stderr", "/proc/self/fd/2"}})
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

    /// A link inside a bind mount exists there already.
    for (const auto & link : links)
    {
        struct stat st{};
        if (0 == ::lstat(link.path.c_str(), &st))
            continue;
        createDirectories(fs::path(link.path).parent_path());
        if (0 != ::symlink(link.target.c_str(), link.path.c_str()))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create the symbolic link {} in the isolated filesystem", link.path);
    }

    /// Relative paths in the configuration keep their meaning: the working directory exists in the new root, empty
    /// unless it is visible. The process must leave the old one anyway, because it is a way out.
    if (!working_directory.empty())
        createDirectories(working_directory);

    if (0 != ::umount2(old_root, MNT_DETACH))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot unmount the original root filesystem");
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
    {
        uint64_t keep = ~uint64_t(0);
        for (const auto capability : dangerous_capabilities)
            keep &= ~(uint64_t(1) << capability);
        restrictCapabilities(keep);
    }

    return rememberStatus(mode, std::move(status));
}

}

#else

namespace DB
{

FilesystemIsolationStatus isolateFilesystem(FilesystemIsolationMode mode, const std::vector<FilesystemIsolationPath> &)
{
    requested_mode.store(static_cast<int>(mode), std::memory_order_relaxed);

    FilesystemIsolationStatus status;
    if (mode == FilesystemIsolationMode::Enforce)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The filesystem isolation is implemented only on Linux");
    if (mode == FilesystemIsolationMode::BestEffort)
        status.not_isolated_reason = "the filesystem isolation is implemented only on Linux";
    return rememberStatus(mode, std::move(status));
}

}

#endif
