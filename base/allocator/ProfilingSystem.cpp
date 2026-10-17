/// The parts of the profiler that talk to the system: backtraces, thread names, the pid (namespace), dump files and
/// their names, `MAPPED_LIBRARIES` (jemalloc: `prof_sys.c`).

#include <allocator/Profiling.h>

#include <allocator/Base.h>
#include <allocator/BufferedWriter.h>
#include <allocator/Format.h>
#include <allocator/MallctlImpl.h>
#include <allocator/Options.h>

#include <cerrno>
#include <climits>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#if defined(__FreeBSD__)
#include <pthread_np.h>
#endif

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

/// jemalloc uses libunwind's `unw_backtrace` (`JEMALLOC_PROF_LIBUNWIND`); ClickHouse links LLVM libunwind.
extern "C" int unw_backtrace(void ** buffer, int size);

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit Mutex profiling_dump_filename_mutex;

/// The fallback allocator profiling functionality will use (`b0`).
constinit Base * profiling_base = nullptr;

namespace
{

/// jemalloc: prof_dump_seq, prof_dump_iseq, prof_dump_mseq, prof_dump_useq
constinit uint64_t profiling_dump_sequence = 0;
constinit uint64_t profiling_dump_interval_sequence = 0;
constinit uint64_t profiling_dump_manual_sequence = 0;
constinit uint64_t profiling_dump_growth_sequence = 0;

/// Set by `prof.prefix` (a base-allocated buffer of `PROFILING_DUMP_FILENAME_LEN` bytes). jemalloc: prof_prefix
constinit char * profiling_prefix = nullptr;

/// This buffer is rather large for stack allocation, so use a single buffer for all profile dumps; protected by
/// `profiling_dump_mutex`. jemalloc: prof_dump_buf
constinit char profiling_dump_buf[PROFILING_DUMP_BUF_SIZE] = {};

}

/// --- Backtraces ----------------------------------------------------------------------------------------------------

/// jemalloc: bt_init
void backtraceInit(ProfilingBacktrace * backtrace, void ** vector)
{
    backtrace->vector = vector;
    backtrace->len = 0;
}

/// jemalloc: prof_backtrace_impl (`JEMALLOC_PROF_LIBUNWIND`)
void profilingBacktraceImpl(void ** vector, unsigned * len, unsigned max_len)
{
    ALLOCATOR_ASSERT(*len == 0);
    ALLOCATOR_ASSERT(vector != nullptr);
    ALLOCATOR_ASSERT(max_len <= PROFILING_BACKTRACE_MAX_LIMIT);

    int num_frames = unw_backtrace(vector, static_cast<int>(max_len));
    if (num_frames <= 0)
        return;
    *len = static_cast<unsigned>(num_frames);
}

/// jemalloc: prof_backtrace
void profilingBacktrace(ThreadState & thread_state, ProfilingBacktrace * backtrace)
{
    ProfilingBacktraceHook backtrace_hook = profilingBacktraceHookGet();
    ALLOCATOR_ASSERT(backtrace_hook != nullptr);

    preReentrancy(thread_state, nullptr);
    backtrace_hook(backtrace->vector, &backtrace->len, options.profiling_backtrace_max);
    postReentrancy(thread_state);
}

/// jemalloc: prof_hooks_init
void profilingHooksInit()
{
    profilingBacktraceHookSet(&profilingBacktraceImpl);
    profilingDumpHookSet(nullptr);
    profilingSampleHookSet(nullptr);
    profilingSampleFreeHookSet(nullptr);
}

/// Nothing to do with libunwind (libgcc's unwinder is never used). jemalloc: prof_unwind_init
void profilingUnwindInit()
{
}

/// --- Thread names --------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_sys_thread_name_read_impl
int profilingSystemThreadNameRead(char * buf, size_t limit)
{
    /// `JEMALLOC_HAVE_PTHREAD_GETNAME_NP` takes precedence over `JEMALLOC_HAVE_PTHREAD_GET_NAME_NP` (FreeBSD ppc64le
    /// has both).
#if (defined(__linux__) && defined(__GLIBC__)) || defined(__APPLE__) || (defined(__FreeBSD__) && defined(__powerpc64__))
    static_assert(config::have_pthread_getname_np);
    return pthread_getname_np(pthread_self(), buf, limit);
#elif defined(__FreeBSD__)
    static_assert(config::have_pthread_get_name_np);
    pthread_get_name_np(pthread_self(), buf, limit);
    return 0;
#else
    static_assert(!config::have_pthread_getname_np && !config::have_pthread_get_name_np);
    (void)buf;
    (void)limit;
    return ENOSYS;
#endif
}

}

/// jemalloc: prof_sys_thread_name_fetch
void profilingSystemThreadNameFetch(ThreadState & thread_state)
{
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return;

    if (profilingSystemThreadNameRead(thread_data->thread_name, PROFILING_THREAD_NAME_MAX_LEN) != 0)
        profilingThreadNameClear(thread_data);

    thread_data->thread_name[PROFILING_THREAD_NAME_MAX_LEN - 1] = '\0';
}

/// --- Process identity ----------------------------------------------------------------------------------------------

/// jemalloc: prof_getpid
int profilingGetPID()
{
    return getpid();
}

namespace
{

/// jemalloc: prof_get_pid_namespace
long profilingGetPIDNamespace()
{
    long result = 0;

    if constexpr (!config::os_darwin)
    {
        char buf[PATH_MAX];
        const char * link_name = config::os_freebsd ? "/proc/curproc/ns/pid" : "/proc/self/ns/pid";
        ssize_t link_length = readlink(link_name, buf, PATH_MAX);

        /// The namespace string is expected to be like pid:[4026531836].
        if (link_length > 0)
        {
            /// Trim the trailing "]".
            buf[link_length - 1] = '\0';
            char * index = strtok(buf, "pid:[");
            result = atol(index);
        }
    }

    return result;
}

/// --- Dump files ----------------------------------------------------------------------------------------------------

/// jemalloc: prof_dump_arg_t
struct ProfilingDumpArg
{
    /// Whether error should be handled locally: if true, then we print out error message as well as abort (if
    /// `opt.abort` is true) when an error occurred, and we also report the error back to the caller in the end; if
    /// false, then we only report the error back to the caller in the end.
    const bool handle_error_locally;
    /// Whether there has been an error in the dumping process, which could have happened either in file opening or in
    /// file writing. When an error has already occurred, we will stop further writing to the file.
    bool error;
    /// File descriptor of the dump file.
    int profiling_dump_fd;
};

/// jemalloc: prof_dump_check_possible_error
ALLOCATOR_FORMAT_PRINTF(3, 4)
void profilingDumpCheckPossibleError(ProfilingDumpArg * arg, bool error_condition, const char * format_string, ...)
{
    ALLOCATOR_ASSERT(!arg->error);
    if (!error_condition)
        return;

    arg->error = true;
    if (!arg->handle_error_locally)
        return;

    va_list args;
    char buf[PROFILING_PRINTF_BUF_SIZE];
    va_start(args, format_string);
    formatV(buf, sizeof(buf), format_string, args);
    va_end(args);
    writeMessage(buf);

    if (options.abort)
        abort();
}

/// jemalloc: prof_dump_open_file_impl
int profilingDumpOpenFile(const char * filename, int mode)
{
    return creat(filename, static_cast<mode_t>(mode));
}

/// jemalloc: prof_dump_open
void profilingDumpOpen(ProfilingDumpArg * arg, const char * filename)
{
    arg->profiling_dump_fd = profilingDumpOpenFile(filename, 0644);
    profilingDumpCheckPossibleError(arg, arg->profiling_dump_fd == -1, "<jemalloc>: failed to open \"%s\"\n", filename);
}

/// jemalloc: prof_dump_flush
void profilingDumpFlush(void * opaque, const char * s)
{
    auto * arg = static_cast<ProfilingDumpArg *>(opaque);
    if (!arg->error)
    {
        ssize_t error = writeFD(arg->profiling_dump_fd, s, strlen(s));
        profilingDumpCheckPossibleError(arg, error == -1, "<jemalloc>: failed to write during heap profile flush\n");
    }
}

/// jemalloc: prof_dump_close
void profilingDumpClose(ProfilingDumpArg * arg)
{
    if (arg->profiling_dump_fd != -1)
        close(arg->profiling_dump_fd);
}

#if defined(__APPLE__)

using MachHeader = struct mach_header_64;
using SegmentCommand = struct segment_command_64;
constexpr uint32_t MH_MAGIC_VALUE = MH_MAGIC_64;
constexpr uint32_t MH_CIGAM_VALUE = MH_CIGAM_64;
constexpr uint32_t LC_SEGMENT_VALUE = LC_SEGMENT_64;

/// jemalloc: prof_dump_dyld_image_vmaddr
void profilingDumpDyldImageVmaddr(BufferedWriter * buf_writer, uint32_t image_index)
{
    const auto * header = reinterpret_cast<const MachHeader *>(_dyld_get_image_header(image_index));
    if (header == nullptr || (header->magic != MH_MAGIC_VALUE && header->magic != MH_CIGAM_VALUE))
    {
        /// Invalid header.
        return;
    }

    intptr_t slide = _dyld_get_image_vmaddr_slide(image_index);
    const char * name = _dyld_get_image_name(image_index);
    const auto * command = reinterpret_cast<const struct load_command *>(reinterpret_cast<const char *>(header) + sizeof(MachHeader));
    for (uint32_t i = 0; command && (i < header->ncmds); ++i)
    {
        if (command->cmd == LC_SEGMENT_VALUE)
        {
            const auto * segment = reinterpret_cast<const SegmentCommand *>(command);
            if (!strcmp(segment->segname, "__TEXT"))
            {
                char buffer[PATH_MAX + 1];
                format(
                    buffer,
                    sizeof(buffer),
                    "%016llx-%016llx: %s\n",
                    static_cast<unsigned long long>(segment->vmaddr + slide),
                    static_cast<unsigned long long>(segment->vmaddr + slide + segment->vmsize),
                    name);
                buf_writer->write(buffer);
                return;
            }
        }
        command = reinterpret_cast<const struct load_command *>(reinterpret_cast<const char *>(command) + command->cmdsize);
    }
}

/// jemalloc: prof_dump_dyld_maps
void profilingDumpDyldMaps(BufferedWriter * buf_writer)
{
    uint32_t image_count = _dyld_image_count();
    for (uint32_t i = 0; i < image_count; ++i)
        profilingDumpDyldImageVmaddr(buf_writer, i);
}

/// jemalloc: prof_dump_maps (Darwin)
void profilingDumpMaps(BufferedWriter * buf_writer)
{
    buf_writer->write("\nMAPPED_LIBRARIES:\n");
    /// No proc map file to read on MacOS, dump dyld maps for backtrace.
    profilingDumpDyldMaps(buf_writer);
}

#else

/// jemalloc: prof_open_maps_internal
ALLOCATOR_FORMAT_PRINTF(1, 2)
int profilingOpenMapsInternal(const char * format_string, ...)
{
    va_list args;
    char filename[PATH_MAX + 1];

    va_start(args, format_string);
    formatV(filename, sizeof(filename), format_string, args);
    va_end(args);

    return open(filename, O_RDONLY | O_CLOEXEC);
}

/// jemalloc: prof_dump_open_maps_impl
int profilingDumpOpenMaps()
{
    int maps_fd;
    if constexpr (config::os_freebsd)
    {
        maps_fd = profilingOpenMapsInternal("/proc/curproc/map");
    }
    else
    {
        int pid = profilingGetPID();

        maps_fd = profilingOpenMapsInternal("/proc/%d/task/%d/maps", pid, pid);
        if (maps_fd == -1)
            maps_fd = profilingOpenMapsInternal("/proc/%d/maps", pid);
    }
    return maps_fd;
}

/// jemalloc: prof_dump_read_maps_cb
ssize_t profilingDumpReadMapsCallback(void * read_callback_argument, void * buf, size_t limit)
{
    int maps_fd = *static_cast<int *>(read_callback_argument);
    ALLOCATOR_ASSERT(maps_fd != -1);
    return readFD(maps_fd, buf, limit);
}

/// jemalloc: prof_dump_maps
void profilingDumpMaps(BufferedWriter * buf_writer)
{
    int maps_fd = profilingDumpOpenMaps();
    if (maps_fd == -1)
        return;

    buf_writer->write("\nMAPPED_LIBRARIES:\n");
    buf_writer->pipe(profilingDumpReadMapsCallback, &maps_fd);
    close(maps_fd);
}

#endif

/// jemalloc: prof_dump
bool profilingDump(ThreadState & thread_state, bool propagate_error, const char * filename, bool leak_check)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return true;

    ProfilingDumpArg arg = {/* handle_error_locally */ !propagate_error, /* error */ false, /* prof_dump_fd */ -1};

    preReentrancy(thread_state, nullptr);
    profiling_dump_mutex.lock(&thread_state);

    profilingDumpOpen(&arg, filename);
    BufferedWriter buf_writer;
    bool error = buf_writer.init(&thread_state, profilingDumpFlush, &arg, profiling_dump_buf, PROFILING_DUMP_BUF_SIZE);
    ALLOCATOR_ASSERT(!error);
    (void)error;
    profilingDumpImpl(thread_state, BufferedWriter::callback, &buf_writer, thread_data, leak_check);
    profilingDumpMaps(&buf_writer);
    buf_writer.terminate(&thread_state);
    profilingDumpClose(&arg);

    ProfilingDumpHook dump_hook = profilingDumpHookGet();
    if (dump_hook != nullptr)
        dump_hook(filename);
    profiling_dump_mutex.unlock(&thread_state);
    postReentrancy(thread_state);

    return arg.error;
}

/// jemalloc: prof_prefix_get
const char * profilingPrefixGet(ThreadState * thread_state)
{
    profiling_dump_filename_mutex.assertOwner(thread_state);

    return profiling_prefix == nullptr ? options.profiling_prefix : profiling_prefix;
}

/// jemalloc: prof_prefix_is_empty
[[maybe_unused]] bool profilingPrefixIsEmpty(ThreadState * thread_state)
{
    MutexLock lock(thread_state, profiling_dump_filename_mutex);
    return profilingPrefixGet(thread_state)[0] == '\0';
}

/// jemalloc: DUMP_FILENAME_BUFSIZE, VSEQ_INVALID
constexpr size_t DUMP_FILENAME_BUF_SIZE = PATH_MAX + 1;
constexpr uint64_t SEQUENCE_INVALID = UINT64_C(0xffffffffffffffff);

/// jemalloc: prof_dump_filename
void profilingDumpFilename(ThreadState & thread_state, char * filename, char v, uint64_t type_sequence)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);
    const char * prefix = profilingPrefixGet(&thread_state);

    auto sequence = static_cast<unsigned long long>(profiling_dump_sequence);
    if (type_sequence != SEQUENCE_INVALID)
    {
        if (options.profiling_pid_namespace)
        {
            /// "<prefix>.<pid_namespace>.<pid>.<seq>.v<vseq>.heap"
            format(
                filename,
                DUMP_FILENAME_BUF_SIZE,
                "%s.%ld.%d.%llu.%c%llu.heap",
                prefix,
                profilingGetPIDNamespace(),
                profilingGetPID(),
                sequence,
                v,
                static_cast<unsigned long long>(type_sequence));
        }
        else
        {
            /// "<prefix>.<pid>.<seq>.v<vseq>.heap"
            format(
                filename,
                DUMP_FILENAME_BUF_SIZE,
                "%s.%d.%llu.%c%llu.heap",
                prefix,
                profilingGetPID(),
                sequence,
                v,
                static_cast<unsigned long long>(type_sequence));
        }
    }
    else
    {
        if (options.profiling_pid_namespace)
        {
            /// "<prefix>.<pid_namespace>.<pid>.<seq>.<v>.heap"
            format(
                filename,
                DUMP_FILENAME_BUF_SIZE,
                "%s.%ld.%d.%llu.%c.heap",
                prefix,
                profilingGetPIDNamespace(),
                profilingGetPID(),
                sequence,
                v);
        }
        else
        {
            /// "<prefix>.<pid>.<seq>.<v>.heap"
            format(filename, DUMP_FILENAME_BUF_SIZE, "%s.%d.%llu.%c.heap", prefix, profilingGetPID(), sequence, v);
        }
    }
    ++profiling_dump_sequence;
}

}

/// `prof_get_default_filename` is only used by the dropped `profiling_log`.

/// jemalloc: prof_fdump_impl
void profilingFinalDumpImpl(ThreadState & thread_state)
{
    char filename[DUMP_FILENAME_BUF_SIZE];

    ALLOCATOR_ASSERT(!profilingPrefixIsEmpty(&thread_state));
    profiling_dump_filename_mutex.lock(&thread_state);
    profilingDumpFilename(thread_state, filename, 'f', SEQUENCE_INVALID);
    profiling_dump_filename_mutex.unlock(&thread_state);
    profilingDump(thread_state, false, filename, options.profiling_leak);
}

/// jemalloc: prof_prefix_set
bool profilingPrefixSet(ThreadState * thread_state, const char * prefix)
{
    mallctl_mutex.assertOwner(thread_state);
    if (prefix == nullptr)
        return true;
    profiling_dump_filename_mutex.lock(thread_state);
    if (profiling_prefix == nullptr)
    {
        profiling_dump_filename_mutex.unlock(thread_state);
        /// Everything is still guarded by `mallctl_mutex`.
        char * buffer = static_cast<char *>(profiling_base->alloc(thread_state, PROFILING_DUMP_FILENAME_LEN, QUANTUM));
        if (buffer == nullptr)
            return true;
        profiling_dump_filename_mutex.lock(thread_state);
        profiling_prefix = buffer;
    }
    ALLOCATOR_ASSERT(profiling_prefix != nullptr);

    strncpy(profiling_prefix, prefix, PROFILING_DUMP_FILENAME_LEN - 1);
    profiling_prefix[PROFILING_DUMP_FILENAME_LEN - 1] = '\0';
    profiling_dump_filename_mutex.unlock(thread_state);

    return false;
}

/// jemalloc: prof_idump_impl
void profilingIntervalDumpImpl(ThreadState & thread_state)
{
    profiling_dump_filename_mutex.lock(&thread_state);
    if (profilingPrefixGet(&thread_state)[0] == '\0')
    {
        profiling_dump_filename_mutex.unlock(&thread_state);
        return;
    }
    char filename[PATH_MAX + 1];
    profilingDumpFilename(thread_state, filename, 'i', profiling_dump_interval_sequence);
    ++profiling_dump_interval_sequence;
    profiling_dump_filename_mutex.unlock(&thread_state);
    profilingDump(thread_state, false, filename, false);
}

/// jemalloc: prof_mdump_impl
bool profilingManualDumpImpl(ThreadState & thread_state, const char * filename)
{
    char filename_buf[DUMP_FILENAME_BUF_SIZE];
    if (filename == nullptr)
    {
        /// No filename specified, so automatically generate one.
        profiling_dump_filename_mutex.lock(&thread_state);
        if (profilingPrefixGet(&thread_state)[0] == '\0')
        {
            profiling_dump_filename_mutex.unlock(&thread_state);
            return true;
        }
        profilingDumpFilename(thread_state, filename_buf, 'm', profiling_dump_manual_sequence);
        ++profiling_dump_manual_sequence;
        profiling_dump_filename_mutex.unlock(&thread_state);
        filename = filename_buf;
    }
    return profilingDump(thread_state, true, filename, false);
}

/// jemalloc: prof_gdump_impl
void profilingGrowthDumpImpl(ThreadState & thread_state)
{
    profiling_dump_filename_mutex.lock(&thread_state);
    if (profilingPrefixGet(&thread_state)[0] == '\0')
    {
        profiling_dump_filename_mutex.unlock(&thread_state);
        return;
    }
    char filename[DUMP_FILENAME_BUF_SIZE];
    profilingDumpFilename(thread_state, filename, 'u', profiling_dump_growth_sequence);
    ++profiling_dump_growth_sequence;
    profiling_dump_filename_mutex.unlock(&thread_state);
    profilingDump(thread_state, false, filename, false);
}

}
