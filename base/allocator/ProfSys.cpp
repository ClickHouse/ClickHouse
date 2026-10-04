/// The parts of the profiler that talk to the system: backtraces, thread names, the pid (namespace), dump files and
/// their names, `MAPPED_LIBRARIES` (jemalloc: `prof_sys.c`).

#include <allocator/Prof.h>

#include <allocator/Base.h>
#include <allocator/BufferedWriter.h>
#include <allocator/CtlImpl.h>
#include <allocator/Format.h>
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
#    include <pthread_np.h>
#endif

#if defined(__APPLE__)
#    include <mach-o/dyld.h>
#endif

/// jemalloc uses libunwind's `unw_backtrace` (`JEMALLOC_PROF_LIBUNWIND`); ClickHouse links LLVM libunwind.
extern "C" int unw_backtrace(void ** buffer, int size);

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit Mutex prof_dump_filename_mtx;

/// The fallback allocator profiling functionality will use (`b0`).
constinit Base * prof_base = nullptr;

namespace
{

/// jemalloc: prof_dump_seq, prof_dump_iseq, prof_dump_mseq, prof_dump_useq
constinit uint64_t prof_dump_seq = 0;
constinit uint64_t prof_dump_iseq = 0;
constinit uint64_t prof_dump_mseq = 0;
constinit uint64_t prof_dump_useq = 0;

/// Set by `prof.prefix` (a base-allocated buffer of `PROF_DUMP_FILENAME_LEN` bytes). jemalloc: prof_prefix
constinit char * prof_prefix = nullptr;

/// This buffer is rather large for stack allocation, so use a single buffer for all profile dumps; protected by
/// `prof_dump_mtx`. jemalloc: prof_dump_buf
constinit char prof_dump_buf[PROF_DUMP_BUFSIZE] = {};

}

/// --- Backtraces ----------------------------------------------------------------------------------------------------

/// jemalloc: bt_init
void btInit(ProfBacktrace * bt, void ** vec)
{
    bt->vec = vec;
    bt->len = 0;
}

/// jemalloc: prof_backtrace_impl (`JEMALLOC_PROF_LIBUNWIND`)
void profBacktraceImpl(void ** vec, unsigned * len, unsigned max_len)
{
    JE_ASSERT(*len == 0);
    JE_ASSERT(vec != nullptr);
    JE_ASSERT(max_len <= PROF_BT_MAX_LIMIT);

    int nframes = unw_backtrace(vec, static_cast<int>(max_len));
    if (nframes <= 0)
        return;
    *len = static_cast<unsigned>(nframes);
}

/// jemalloc: prof_backtrace
void profBacktrace(ThreadState & tsd, ProfBacktrace * bt)
{
    ProfBacktraceHook backtrace_hook = profBacktraceHookGet();
    JE_ASSERT(backtrace_hook != nullptr);

    preReentrancy(tsd, nullptr);
    backtrace_hook(bt->vec, &bt->len, opt.prof_bt_max);
    postReentrancy(tsd);
}

/// jemalloc: prof_hooks_init
void profHooksInit()
{
    profBacktraceHookSet(&profBacktraceImpl);
    profDumpHookSet(nullptr);
    profSampleHookSet(nullptr);
    profSampleFreeHookSet(nullptr);
}

/// Nothing to do with libunwind (libgcc's unwinder is never used). jemalloc: prof_unwind_init
void profUnwindInit()
{
}

/// --- Thread names --------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_sys_thread_name_read_impl
int profSysThreadNameRead(char * buf, size_t limit)
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
void profSysThreadNameFetch(ThreadState & tsd)
{
    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return;

    if (profSysThreadNameRead(tdata->thread_name, PROF_THREAD_NAME_MAX_LEN) != 0)
        profThreadNameClear(tdata);

    tdata->thread_name[PROF_THREAD_NAME_MAX_LEN - 1] = '\0';
}

/// --- Process identity ----------------------------------------------------------------------------------------------

/// jemalloc: prof_getpid
int profGetpid()
{
    return getpid();
}

namespace
{

/// jemalloc: prof_get_pid_namespace
long profGetPidNamespace()
{
    long ret = 0;

    if constexpr (!config::os_darwin)
    {
        char buf[PATH_MAX];
        const char * linkname = config::os_freebsd ? "/proc/curproc/ns/pid" : "/proc/self/ns/pid";
        ssize_t linklen = readlink(linkname, buf, PATH_MAX);

        /// The namespace string is expected to be like pid:[4026531836].
        if (linklen > 0)
        {
            /// Trim the trailing "]".
            buf[linklen - 1] = '\0';
            char * index = strtok(buf, "pid:[");
            ret = atol(index);
        }
    }

    return ret;
}

/// --- Dump files ----------------------------------------------------------------------------------------------------

/// jemalloc: prof_dump_arg_t
struct ProfDumpArg
{
    /// Whether error should be handled locally: if true, then we print out error message as well as abort (if
    /// `opt.abort` is true) when an error occurred, and we also report the error back to the caller in the end; if
    /// false, then we only report the error back to the caller in the end.
    const bool handle_error_locally;
    /// Whether there has been an error in the dumping process, which could have happened either in file opening or in
    /// file writing. When an error has already occurred, we will stop further writing to the file.
    bool error;
    /// File descriptor of the dump file.
    int prof_dump_fd;
};

/// jemalloc: prof_dump_check_possible_error
JE_FORMAT_PRINTF(3, 4)
void profDumpCheckPossibleError(ProfDumpArg * arg, bool err_cond, const char * fmt, ...)
{
    JE_ASSERT(!arg->error);
    if (!err_cond)
        return;

    arg->error = true;
    if (!arg->handle_error_locally)
        return;

    va_list ap;
    char buf[PROF_PRINTF_BUFSIZE];
    va_start(ap, fmt);
    formatV(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    writeMessage(buf);

    if (opt.abort)
        abort();
}

/// jemalloc: prof_dump_open_file_impl
int profDumpOpenFile(const char * filename, int mode)
{
    return creat(filename, static_cast<mode_t>(mode));
}

/// jemalloc: prof_dump_open
void profDumpOpen(ProfDumpArg * arg, const char * filename)
{
    arg->prof_dump_fd = profDumpOpenFile(filename, 0644);
    profDumpCheckPossibleError(arg, arg->prof_dump_fd == -1, "<jemalloc>: failed to open \"%s\"\n", filename);
}

/// jemalloc: prof_dump_flush
void profDumpFlush(void * opaque, const char * s)
{
    auto * arg = static_cast<ProfDumpArg *>(opaque);
    if (!arg->error)
    {
        ssize_t err = writeFd(arg->prof_dump_fd, s, strlen(s));
        profDumpCheckPossibleError(arg, err == -1, "<jemalloc>: failed to write during heap profile flush\n");
    }
}

/// jemalloc: prof_dump_close
void profDumpClose(ProfDumpArg * arg)
{
    if (arg->prof_dump_fd != -1)
        close(arg->prof_dump_fd);
}

#if defined(__APPLE__)

using MachHeader = struct mach_header_64;
using SegmentCommand = struct segment_command_64;
constexpr uint32_t MH_MAGIC_VALUE = MH_MAGIC_64;
constexpr uint32_t MH_CIGAM_VALUE = MH_CIGAM_64;
constexpr uint32_t LC_SEGMENT_VALUE = LC_SEGMENT_64;

/// jemalloc: prof_dump_dyld_image_vmaddr
void profDumpDyldImageVmaddr(BufferedWriter * buf_writer, uint32_t image_index)
{
    const auto * header = reinterpret_cast<const MachHeader *>(_dyld_get_image_header(image_index));
    if (header == nullptr || (header->magic != MH_MAGIC_VALUE && header->magic != MH_CIGAM_VALUE))
    {
        /// Invalid header.
        return;
    }

    intptr_t slide = _dyld_get_image_vmaddr_slide(image_index);
    const char * name = _dyld_get_image_name(image_index);
    const auto * load_cmd = reinterpret_cast<const struct load_command *>(reinterpret_cast<const char *>(header) + sizeof(MachHeader));
    for (uint32_t i = 0; load_cmd && (i < header->ncmds); ++i)
    {
        if (load_cmd->cmd == LC_SEGMENT_VALUE)
        {
            const auto * segment_cmd = reinterpret_cast<const SegmentCommand *>(load_cmd);
            if (!strcmp(segment_cmd->segname, "__TEXT"))
            {
                char buffer[PATH_MAX + 1];
                format(
                    buffer,
                    sizeof(buffer),
                    "%016llx-%016llx: %s\n",
                    static_cast<unsigned long long>(segment_cmd->vmaddr + slide),
                    static_cast<unsigned long long>(segment_cmd->vmaddr + slide + segment_cmd->vmsize),
                    name);
                buf_writer->write(buffer);
                return;
            }
        }
        load_cmd = reinterpret_cast<const struct load_command *>(reinterpret_cast<const char *>(load_cmd) + load_cmd->cmdsize);
    }
}

/// jemalloc: prof_dump_dyld_maps
void profDumpDyldMaps(BufferedWriter * buf_writer)
{
    uint32_t image_count = _dyld_image_count();
    for (uint32_t i = 0; i < image_count; ++i)
        profDumpDyldImageVmaddr(buf_writer, i);
}

/// jemalloc: prof_dump_maps (Darwin)
void profDumpMaps(BufferedWriter * buf_writer)
{
    buf_writer->write("\nMAPPED_LIBRARIES:\n");
    /// No proc map file to read on MacOS, dump dyld maps for backtrace.
    profDumpDyldMaps(buf_writer);
}

#else

/// jemalloc: prof_open_maps_internal
JE_FORMAT_PRINTF(1, 2)
int profOpenMapsInternal(const char * fmt, ...)
{
    va_list ap;
    char filename[PATH_MAX + 1];

    va_start(ap, fmt);
    formatV(filename, sizeof(filename), fmt, ap);
    va_end(ap);

    return open(filename, O_RDONLY | O_CLOEXEC);
}

/// jemalloc: prof_dump_open_maps_impl
int profDumpOpenMaps()
{
    int mfd;
    if constexpr (config::os_freebsd)
    {
        mfd = profOpenMapsInternal("/proc/curproc/map");
    }
    else
    {
        int pid = profGetpid();

        mfd = profOpenMapsInternal("/proc/%d/task/%d/maps", pid, pid);
        if (mfd == -1)
            mfd = profOpenMapsInternal("/proc/%d/maps", pid);
    }
    return mfd;
}

/// jemalloc: prof_dump_read_maps_cb
ssize_t profDumpReadMapsCb(void * read_cbopaque, void * buf, size_t limit)
{
    int mfd = *static_cast<int *>(read_cbopaque);
    JE_ASSERT(mfd != -1);
    return readFd(mfd, buf, limit);
}

/// jemalloc: prof_dump_maps
void profDumpMaps(BufferedWriter * buf_writer)
{
    int mfd = profDumpOpenMaps();
    if (mfd == -1)
        return;

    buf_writer->write("\nMAPPED_LIBRARIES:\n");
    buf_writer->pipe(profDumpReadMapsCb, &mfd);
    close(mfd);
}

#endif

/// jemalloc: prof_dump
bool profDump(ThreadState & tsd, bool propagate_err, const char * filename, bool leakcheck)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return true;

    ProfDumpArg arg = {/* handle_error_locally */ !propagate_err, /* error */ false, /* prof_dump_fd */ -1};

    preReentrancy(tsd, nullptr);
    prof_dump_mtx.lock(&tsd);

    profDumpOpen(&arg, filename);
    BufferedWriter buf_writer;
    bool err = buf_writer.init(&tsd, profDumpFlush, &arg, prof_dump_buf, PROF_DUMP_BUFSIZE);
    JE_ASSERT(!err);
    (void)err;
    profDumpImpl(tsd, BufferedWriter::callback, &buf_writer, tdata, leakcheck);
    profDumpMaps(&buf_writer);
    buf_writer.terminate(&tsd);
    profDumpClose(&arg);

    ProfDumpHook dump_hook = profDumpHookGet();
    if (dump_hook != nullptr)
        dump_hook(filename);
    prof_dump_mtx.unlock(&tsd);
    postReentrancy(tsd);

    return arg.error;
}

/// jemalloc: prof_prefix_get
const char * profPrefixGet(ThreadState * tsdn)
{
    prof_dump_filename_mtx.assertOwner(tsdn);

    return prof_prefix == nullptr ? opt.prof_prefix : prof_prefix;
}

/// jemalloc: prof_prefix_is_empty
[[maybe_unused]] bool profPrefixIsEmpty(ThreadState * tsdn)
{
    MutexLock lock(tsdn, prof_dump_filename_mtx);
    return profPrefixGet(tsdn)[0] == '\0';
}

/// jemalloc: DUMP_FILENAME_BUFSIZE, VSEQ_INVALID
constexpr size_t DUMP_FILENAME_BUFSIZE = PATH_MAX + 1;
constexpr uint64_t VSEQ_INVALID = UINT64_C(0xffffffffffffffff);

/// jemalloc: prof_dump_filename
void profDumpFilename(ThreadState & tsd, char * filename, char v, uint64_t vseq)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);
    const char * prefix = profPrefixGet(&tsd);

    auto seq = static_cast<unsigned long long>(prof_dump_seq);
    if (vseq != VSEQ_INVALID)
    {
        if (opt.prof_pid_namespace)
        {
            /// "<prefix>.<pid_namespace>.<pid>.<seq>.v<vseq>.heap"
            format(
                filename,
                DUMP_FILENAME_BUFSIZE,
                "%s.%ld.%d.%llu.%c%llu.heap",
                prefix,
                profGetPidNamespace(),
                profGetpid(),
                seq,
                v,
                static_cast<unsigned long long>(vseq));
        }
        else
        {
            /// "<prefix>.<pid>.<seq>.v<vseq>.heap"
            format(
                filename,
                DUMP_FILENAME_BUFSIZE,
                "%s.%d.%llu.%c%llu.heap",
                prefix,
                profGetpid(),
                seq,
                v,
                static_cast<unsigned long long>(vseq));
        }
    }
    else
    {
        if (opt.prof_pid_namespace)
        {
            /// "<prefix>.<pid_namespace>.<pid>.<seq>.<v>.heap"
            format(filename, DUMP_FILENAME_BUFSIZE, "%s.%ld.%d.%llu.%c.heap", prefix, profGetPidNamespace(), profGetpid(), seq, v);
        }
        else
        {
            /// "<prefix>.<pid>.<seq>.<v>.heap"
            format(filename, DUMP_FILENAME_BUFSIZE, "%s.%d.%llu.%c.heap", prefix, profGetpid(), seq, v);
        }
    }
    ++prof_dump_seq;
}

}

/// `prof_get_default_filename` is only used by the dropped `prof_log`.

/// jemalloc: prof_fdump_impl
void profFdumpImpl(ThreadState & tsd)
{
    char filename[DUMP_FILENAME_BUFSIZE];

    JE_ASSERT(!profPrefixIsEmpty(&tsd));
    prof_dump_filename_mtx.lock(&tsd);
    profDumpFilename(tsd, filename, 'f', VSEQ_INVALID);
    prof_dump_filename_mtx.unlock(&tsd);
    profDump(tsd, false, filename, opt.prof_leak);
}

/// jemalloc: prof_prefix_set
bool profPrefixSet(ThreadState * tsdn, const char * prefix)
{
    ctl_mtx.assertOwner(tsdn);
    if (prefix == nullptr)
        return true;
    prof_dump_filename_mtx.lock(tsdn);
    if (prof_prefix == nullptr)
    {
        prof_dump_filename_mtx.unlock(tsdn);
        /// Everything is still guarded by `ctl_mtx`.
        char * buffer = static_cast<char *>(prof_base->alloc(tsdn, PROF_DUMP_FILENAME_LEN, QUANTUM));
        if (buffer == nullptr)
            return true;
        prof_dump_filename_mtx.lock(tsdn);
        prof_prefix = buffer;
    }
    JE_ASSERT(prof_prefix != nullptr);

    strncpy(prof_prefix, prefix, PROF_DUMP_FILENAME_LEN - 1);
    prof_prefix[PROF_DUMP_FILENAME_LEN - 1] = '\0';
    prof_dump_filename_mtx.unlock(tsdn);

    return false;
}

/// jemalloc: prof_idump_impl
void profIdumpImpl(ThreadState & tsd)
{
    prof_dump_filename_mtx.lock(&tsd);
    if (profPrefixGet(&tsd)[0] == '\0')
    {
        prof_dump_filename_mtx.unlock(&tsd);
        return;
    }
    char filename[PATH_MAX + 1];
    profDumpFilename(tsd, filename, 'i', prof_dump_iseq);
    ++prof_dump_iseq;
    prof_dump_filename_mtx.unlock(&tsd);
    profDump(tsd, false, filename, false);
}

/// jemalloc: prof_mdump_impl
bool profMdumpImpl(ThreadState & tsd, const char * filename)
{
    char filename_buf[DUMP_FILENAME_BUFSIZE];
    if (filename == nullptr)
    {
        /// No filename specified, so automatically generate one.
        prof_dump_filename_mtx.lock(&tsd);
        if (profPrefixGet(&tsd)[0] == '\0')
        {
            prof_dump_filename_mtx.unlock(&tsd);
            return true;
        }
        profDumpFilename(tsd, filename_buf, 'm', prof_dump_mseq);
        ++prof_dump_mseq;
        prof_dump_filename_mtx.unlock(&tsd);
        filename = filename_buf;
    }
    return profDump(tsd, true, filename, false);
}

/// jemalloc: prof_gdump_impl
void profGdumpImpl(ThreadState & tsd)
{
    prof_dump_filename_mtx.lock(&tsd);
    if (profPrefixGet(&tsd)[0] == '\0')
    {
        prof_dump_filename_mtx.unlock(&tsd);
        return;
    }
    char filename[DUMP_FILENAME_BUFSIZE];
    profDumpFilename(tsd, filename, 'u', prof_dump_useq);
    ++prof_dump_useq;
    prof_dump_filename_mtx.unlock(&tsd);
    profDump(tsd, false, filename, false);
}

}
