#include <Common/EnvironmentChecks.h>
#include <Common/IO.h>

#include "config.h"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <charconv>
#include <cerrno>
#include <csignal>
#include <csetjmp>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#if defined(OS_LINUX) && !defined(USE_MUSL)
#include <link.h>
#include <sys/auxv.h>
#include <sys/sysmacros.h>
#endif

#if !defined(USE_MUSL)

namespace
{

/// musl - and so Emscripten - spells this `#define stderr (stderr)`, which
/// `-Wdisabled-macro-expansion` reports at every use.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
template <typename... Args>
[[noreturn]] void printErrorAndExit(fmt::format_string<Args...> format, Args &&... args)
{
    fmt::print(stderr, format, std::forward<Args>(args)...);
    fmt::print(stderr, "\n");
    _exit(1);
}
#pragma clang diagnostic pop

#if defined(OS_LINUX)

constexpr auto injected_objects_are_not_supported_message
    = "ClickHouse does not support running with injected shared objects, because they can interpose "
      "memory allocation, threading and other functions, which leads to crashes or wrong results.";

/// Reads the whole file. Returns false if it does not exist.
bool readFile(const char * path, std::string & content)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
    {
        if (errno == ENOENT)
            return false;
        printErrorAndExit("Cannot open {}: {}", path, strerror(errno)); // NOLINT(concurrency-mt-unsafe)
    }

    char buf[4096];
    while (true)
    {
        ssize_t res = read(fd, buf, sizeof(buf));
        if (res < 0)
        {
            if (errno == EINTR)
                continue;
            printErrorAndExit("Cannot read {}: {}", path, strerror(errno)); // NOLINT(concurrency-mt-unsafe)
        }
        if (res == 0)
            break;
        content.append(buf, static_cast<size_t>(res));
    }
    if (0 != close(fd))
        printErrorAndExit("Cannot close {}: {}", path, strerror(errno)); // NOLINT(concurrency-mt-unsafe)
    return true;
}

/// The dynamic loader of glibc reads `/etc/ld.so.preload` and loads every shared object listed there
/// into every dynamically linked process, exactly as if it was listed in `LD_PRELOAD`.
/// Unlike the environment variable, we cannot get rid of it by re-executing ourselves, so we refuse to run.
void checkPreloadConfigurationFile()
{
    constexpr auto path = "/etc/ld.so.preload";

    /// The loader skips the file if it is not readable, see `rtld.c`.
    if (0 != access(path, R_OK))
        return;

    std::string content;
    if (!readFile(path, content))
        return;

    /// Drop comments: from `#` until the end of the line.
    for (size_t pos = content.find('#'); pos != std::string::npos; pos = content.find('#', pos))
    {
        size_t end = content.find('\n', pos);
        if (end == std::string::npos)
            end = content.size();
        content.replace(pos, end - pos, end - pos, ' ');
    }

    /// The same separators as in glibc's `rtld.c`.
    constexpr std::string_view separators = " \t\n:";
    std::vector<std::string_view> entries;
    std::string_view rest = content;
    while (true)
    {
        size_t begin = rest.find_first_not_of(separators);
        if (begin == std::string_view::npos)
            break;
        rest.remove_prefix(begin);
        size_t length = std::min(rest.find_first_of(separators), rest.size());
        entries.push_back(rest.substr(0, length));
        rest.remove_prefix(length);
    }

    if (!entries.empty())
        printErrorAndExit(
            "The file {} lists shared objects to inject into every process: {}. {} Remove the entries from {}.",
            path, fmt::join(entries, ", "), injected_objects_are_not_supported_message, path);
}


/// Returns the address of the ELF header of the vDSO, or zero if there is no vDSO.
uintptr_t getVDSOAddress()
{
    if (uintptr_t address = getauxval(AT_SYSINFO_EHDR))
        return address;

    /// `getauxval` returns zero for `AT_SYSINFO_EHDR` in some cases, although the kernel maps the vDSO,
    /// e.g., on AArch64 with glibc 2.35 when the program is started by invoking the dynamic loader explicitly
    /// (`/lib/ld-linux-aarch64.so.1 ./clickhouse`). Read the auxiliary vector provided by the kernel directly.
    std::string auxv;
    /// `/proc` may be not mounted, e.g., in a chroot.
    if (!readFile("/proc/self/auxv", auxv))
        return 0;

    for (size_t pos = 0; pos + sizeof(ElfW(auxv_t)) <= auxv.size(); pos += sizeof(ElfW(auxv_t)))
    {
        ElfW(auxv_t) entry;
        memcpy(&entry, auxv.data() + pos, sizeof(entry));
        if (entry.a_type == AT_NULL)
            break;
        if (entry.a_type == AT_SYSINFO_EHDR)
            return entry.a_un.a_val;
    }
    return 0;
}

struct LoadedObject
{
    std::string_view path;
    /// `DT_SONAME` if present, otherwise the file name.
    std::string_view name;
    std::vector<std::string_view> needed;
    /// The address of the first loadable segment.
    uintptr_t load_address = 0;
    bool is_main_executable = false;
    bool is_vdso = false;
};

/// The dynamic loader relocates the addresses in the dynamic section in place,
/// unless the section is read-only (e.g., for vDSO, or on some architectures).
/// The same heuristic is used in `SymbolIndex.cpp`.
uintptr_t correctAddress(uintptr_t base, uintptr_t ptr)
{
    return ptr > base ? ptr : base + ptr;
}

struct LoadedObjects
{
    std::vector<LoadedObject> objects;
    uintptr_t vdso_address = 0;
};

int collectLoadedObject(dl_phdr_info * info, size_t, void * data)
{
    auto & state = *static_cast<LoadedObjects *>(data);
    auto & objects = state.objects;

    LoadedObject object;
    object.path = info->dlpi_name ? info->dlpi_name : "";
    /// The first object is always the main executable.
    object.is_main_executable = objects.empty();

    if (uintptr_t vdso_ehdr = state.vdso_address)
    {
        const auto * ehdr = reinterpret_cast<const ElfW(Ehdr) *>(vdso_ehdr);
        object.is_vdso = reinterpret_cast<uintptr_t>(info->dlpi_phdr) == vdso_ehdr + ehdr->e_phoff;
    }

    for (size_t i = 0; i < info->dlpi_phnum; ++i)
    {
        if (info->dlpi_phdr[i].p_type == PT_LOAD && !object.load_address)
            object.load_address = info->dlpi_addr + info->dlpi_phdr[i].p_vaddr;
    }

    for (size_t i = 0; i < info->dlpi_phnum; ++i)
    {
        if (info->dlpi_phdr[i].p_type != PT_DYNAMIC)
            continue;

        const auto * dyn_begin = reinterpret_cast<const ElfW(Dyn) *>(info->dlpi_addr + info->dlpi_phdr[i].p_vaddr);

        const char * strtab = nullptr;
        for (const auto * dyn = dyn_begin; dyn->d_tag != DT_NULL; ++dyn)
            if (dyn->d_tag == DT_STRTAB)
                strtab = reinterpret_cast<const char *>(correctAddress(info->dlpi_addr, dyn->d_un.d_ptr));

        if (!strtab)
            break;

        for (const auto * dyn = dyn_begin; dyn->d_tag != DT_NULL; ++dyn)
        {
            if (dyn->d_tag == DT_NEEDED)
                object.needed.emplace_back(strtab + dyn->d_un.d_val);
            else if (dyn->d_tag == DT_SONAME)
                object.name = strtab + dyn->d_un.d_val;
        }
        break;
    }

    if (object.name.empty())
    {
        size_t slash = object.path.rfind('/');
        object.name = slash == std::string_view::npos ? object.path : object.path.substr(slash + 1);
    }

    objects.push_back(std::move(object));
    return 0;
}

/// Every shared object in our address space must be either the main executable, the vDSO,
/// or a (transitive) dependency of the main executable, listed in `DT_NEEDED`.
/// Anything else was injected - with `LD_PRELOAD`, `/etc/ld.so.preload`, or loaded with `dlopen`, which we don't use
/// (except for the OpenSSL FIPS provider, see below).
/// This check has to run early in `main`, before glibc loads its own plugins on demand,
/// such as NSS modules or `libgcc_s` for unwinding on thread cancellation.
/// Returns the addresses of all loaded objects.
std::vector<uintptr_t> checkLoadedObjects()
{
    LoadedObjects state;
    state.vdso_address = getVDSOAddress();
    dl_iterate_phdr(collectLoadedObject, &state);
    const auto & objects = state.objects;

    std::vector<bool> expected(objects.size());
    std::vector<std::string_view> expected_names;

    auto mark_expected = [&](size_t i)
    {
        expected[i] = true;
        expected_names.insert(expected_names.end(), objects[i].needed.begin(), objects[i].needed.end());
    };

    /// In a build with OpenSSL FIPS, the FIPS provider is loaded with `dlopen` (which is allowed only in this build,
    /// see `programs/main.cpp`) according to the OpenSSL configuration. It happens in the initialization
    /// of OpenSSL by a global constructor, before `main`. The provider is not a dependency of the binary.
    auto is_openssl_fips_provider = []([[maybe_unused]] const LoadedObject & object)
    {
#if USE_OPENSSL_FIPS
        return object.name == "fips.so";
#else
        return false;
#endif
    };

    for (size_t i = 0; i < objects.size(); ++i)
        if (objects[i].is_main_executable || objects[i].is_vdso || is_openssl_fips_provider(objects[i]))
            mark_expected(i);

    /// Compute the transitive closure of dependencies. The number of objects is tiny.
    for (bool changed = true; changed;)
    {
        changed = false;
        for (size_t i = 0; i < objects.size(); ++i)
        {
            if (!expected[i] && std::ranges::find(expected_names, objects[i].name) != expected_names.end())
            {
                mark_expected(i);
                changed = true;
            }
        }
    }

    auto display_name = [](const LoadedObject & object) { return object.path.empty() ? object.name : object.path; };

    /// The dynamic loader resolves a dependency by its name to the first loaded object with this name,
    /// so in a clean process every name is unique. An injected object can claim the name of a genuine dependency
    /// (e.g., `ld.so --preload /lib/libm.so.6:/tmp/injected.so`, where `injected.so` has `DT_SONAME` `libm.so.6`),
    /// and it would be accepted by the name-based closure above. Refuse to run if two loaded objects have the same name.
    std::vector<std::string_view> duplicates;
    for (size_t i = 0; i < objects.size(); ++i)
    {
        if (objects[i].is_main_executable || objects[i].name.empty())
            continue;
        for (size_t j = 0; j < objects.size(); ++j)
        {
            if (j != i && !objects[j].is_main_executable && objects[j].name == objects[i].name)
            {
                duplicates.push_back(display_name(objects[i]));
                break;
            }
        }
    }

    if (!duplicates.empty())
        printErrorAndExit(
            "Several shared objects with the same name are loaded into the process: {}. "
            "Some of them were possibly injected with `LD_PRELOAD` or `/etc/ld.so.preload`. {}",
            fmt::join(duplicates, ", "), injected_objects_are_not_supported_message);

    std::vector<std::string_view> unexpected;
    for (size_t i = 0; i < objects.size(); ++i)
        if (!expected[i])
            unexpected.push_back(display_name(objects[i]));

    if (!unexpected.empty())
        printErrorAndExit(
            "Unexpected shared objects are loaded into the process: {}. They are not dependencies of the ClickHouse binary "
            "and were possibly injected with `LD_PRELOAD` or `/etc/ld.so.preload`. {}",
            fmt::join(unexpected, ", "), injected_objects_are_not_supported_message);

    std::vector<uintptr_t> addresses;
    for (const auto & object : objects)
        addresses.push_back(object.load_address);
    return addresses;
}

struct Mapping
{
    uintptr_t begin = 0;
    uintptr_t end = 0;
    bool executable = false;
    dev_t device = 0;
    ino_t inode = 0;
    std::string_view path;

    bool isSameFile(const Mapping & other) const { return device == other.device && inode == other.inode; }
};

/// Parses a number in the given base at the beginning of `str` and removes it from `str`.
template <typename T>
bool parseNumber(std::string_view & str, T & value, int base)
{
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value, base);
    if (ec != std::errc() || ptr == str.data())
        return false;
    str.remove_prefix(static_cast<size_t>(ptr - str.data()));
    return true;
}

bool skipChar(std::string_view & str, char c)
{
    if (str.empty() || str.front() != c)
        return false;
    str.remove_prefix(1);
    return true;
}

void skipSpaces(std::string_view & str)
{
    while (!str.empty() && str.front() == ' ')
        str.remove_prefix(1);
}

/// Parses a line of `/proc/self/maps` in the format: begin-end perms offset major:minor inode pathname
bool parseMapsLine(std::string_view line, Mapping & mapping)
{
    uint64_t offset = 0;
    unsigned major = 0;
    unsigned minor = 0;
    uint64_t inode = 0;

    if (!parseNumber(line, mapping.begin, 16) || !skipChar(line, '-') || !parseNumber(line, mapping.end, 16) || !skipChar(line, ' '))
        return false;

    if (line.size() < 5 || line[4] != ' ')
        return false;
    mapping.executable = line[2] == 'x';
    line.remove_prefix(5);

    if (!parseNumber(line, offset, 16) || !skipChar(line, ' ')
        || !parseNumber(line, major, 16) || !skipChar(line, ':') || !parseNumber(line, minor, 16) || !skipChar(line, ' ')
        || !parseNumber(line, inode, 10))
        return false;

    skipSpaces(line);
    mapping.device = makedev(major, minor);
    mapping.inode = static_cast<ino_t>(inode);
    mapping.path = line;
    return true;
}

/// Objects can be loaded outside of the main namespace of the dynamic loader, e.g., auditing libraries (`LD_AUDIT`, `--audit`),
/// or mapped into memory manually. They are not visible to `dl_iterate_phdr`, so we also check that
/// every file-backed executable memory mapping belongs to the file of one of the loaded objects.
void checkExecutableMappings(const std::vector<uintptr_t> & object_addresses)
{
    std::string maps;
    /// `/proc` may be not mounted, e.g., in a chroot.
    if (!readFile("/proc/self/maps", maps))
        return;

    std::vector<Mapping> mappings;
    std::string_view rest = maps;
    while (!rest.empty())
    {
        size_t end_of_line = std::min(rest.find('\n'), rest.size());
        std::string_view line = rest.substr(0, end_of_line);
        rest.remove_prefix(std::min(end_of_line + 1, rest.size()));

        Mapping mapping;
        if (!parseMapsLine(line, mapping))
            printErrorAndExit("Cannot parse a line of /proc/self/maps: {}", line);
        mappings.push_back(mapping);
    }

    std::vector<const Mapping *> expected_files;
    for (uintptr_t address : object_addresses)
    {
        auto it = std::ranges::find_if(mappings, [&](const Mapping & mapping) { return mapping.begin <= address && address < mapping.end; });
        if (it == mappings.end())
            printErrorAndExit("Cannot find the memory mapping of a loaded object at address {:#x} in /proc/self/maps", address);
        expected_files.push_back(&*it);
    }

    std::vector<std::string_view> unexpected;
    for (const auto & mapping : mappings)
    {
        /// Anonymous mappings and vDSO have zero inode.
        if (!mapping.executable || mapping.inode == 0)
            continue;
        if (std::ranges::none_of(expected_files, [&](const Mapping * expected) { return mapping.isSameFile(*expected); }))
            unexpected.push_back(mapping.path);
    }

    if (!unexpected.empty())
        printErrorAndExit(
            "Unexpected files are mapped into the executable memory of the process: {}. "
            "They are not dependencies of the ClickHouse binary and were possibly injected with `LD_AUDIT` or by another program. {}",
            fmt::join(unexpected, ", "), injected_objects_are_not_supported_message);
}

#endif

}

/// NOTE: We will migrate to full static linking or our own dynamic loader to make this code obsolete.
void checkHarmfulEnvironment(char ** argv)
{
    std::initializer_list<const char *> harmful_env_variables = {
        /// The list is a selection from "man ld-linux".
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "LD_ORIGIN_PATH",
        "LD_AUDIT",
        "LD_DYNAMIC_WEAK",
        /// The list is a selection from "man dyld" (osx).
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
        "DYLD_VERSIONED_LIBRARY_PATH",
        "DYLD_INSERT_LIBRARIES",
    };

    bool require_reexec = false;
    for (const auto * var : harmful_env_variables)
    {
        if (const char * value = getenv(var); value && value[0]) // NOLINT(concurrency-mt-unsafe)
        {
            /// NOTE: setenv() is used over unsetenv() since unsetenv() marked as harmful
            if (setenv(var, "", true)) // NOLINT(concurrency-mt-unsafe) // this is safe if not called concurrently
                printErrorAndExit("Cannot override {} environment variable", var);
            require_reexec = true;
        }
    }

    if (require_reexec)
    {
        /// Use execvp() over execv() to search in PATH.
        ///
        /// This should be safe, since:
        /// - if argv[0] is relative path - it is OK
        /// - if argv[0] has only basename, the it will search in PATH, like shell will do.
        ///
        /// Also note, that this (search in PATH) because there is no easy and
        /// portable way to get absolute path of argv[0].
        /// - on linux there is /proc/self/exec and AT_EXECFN
        /// - but on other OSes there is no such thing (especially on OSX).
        ///
        /// And since static linking will be done someday anyway,
        /// let's not pollute the code base with special cases.
        int error = execvp(argv[0], argv);
        _exit(error);
    }

    /// After the re-execution, the environment variables are clean,
    /// but the objects can still be injected by the system-wide configuration.
#if defined(OS_LINUX)
    checkPreloadConfigurationFile();
    checkExecutableMappings(checkLoadedObjects());
#endif
}
#endif
