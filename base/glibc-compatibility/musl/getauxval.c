#include "atomic.h"
#include <sys/auxv.h>
#include <fcntl.h> // open
#include <sys/stat.h> // O_RDONLY
#include <unistd.h> // read, close
#include <stdlib.h> // ssize_t
#include <stdio.h> // perror, fprintf
#include <link.h> // ElfW
#include <errno.h>

#include "syscall.h"

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#endif

#define ARRAY_SIZE(a) sizeof((a))/sizeof((a[0]))

/// Suppress TSan since it is possible for this code to be called from multiple threads,
/// and initialization is safe to be done multiple times from multiple threads.
#define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))

// We don't have libc struct available here.
// Compute aux vector manually (from /proc/self/auxv).
//
// Right now there is only 51 AT_* constants,
// so 64 should be enough until this implementation will be replaced with musl.
static unsigned long __auxv_procfs[64];
static unsigned long __auxv_secure = 0;
// Common
static unsigned long * __auxv_environ = NULL;

static void * volatile getauxval_func;

static unsigned long  __auxv_init_environ(unsigned long type);

//
// auxv from procfs interface
//
ssize_t __retry_read(int fd, void * buf, size_t count)
{
    for (;;)
    {
        // We cannot use the read syscall as it will be intercept by sanitizers, which aren't
        // initialized yet. Emit syscall directly.
        ssize_t ret = __syscall_ret(__syscall(SYS_read, fd, buf, count));
        if (ret == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("Cannot read /proc/self/auxv");
            abort();
        }
        return ret;
    }
}
unsigned long NO_SANITIZE_THREAD __getauxval_procfs(unsigned long type)
{
    if (type == AT_SECURE)
    {
        return __auxv_secure;
    }

    if (type >= ARRAY_SIZE(__auxv_procfs))
    {
        errno = ENOENT;
        return 0;
    }

    return __auxv_procfs[type];
}
static unsigned long NO_SANITIZE_THREAD __auxv_init_procfs(unsigned long type)
{
#if defined(__x86_64__) && defined(__has_feature)
#    if __has_feature(memory_sanitizer) || __has_feature(thread_sanitizer)
    /// Sanitizers are not compatible with high ASLR entropy, which is the default on modern Linux distributions, and
    /// to workaround this limitation, TSAN and MSAN (couldn't see other sanitizers doing the same), re-exec the binary
    /// without ASLR (see https://github.com/llvm/llvm-project/commit/0784b1eefa36d4acbb0dacd2d18796e26313b6c5)

    /// The problem we face is that, in order to re-exec, the sanitizer wants to use the original pathname in the call
    /// and to get its value it uses getauxval (https://github.com/llvm/llvm-project/blob/20eff684203287828d6722fc860b9d3621429542/compiler-rt/lib/sanitizer_common/sanitizer_linux_libcdep.cpp#L985-L988).
    /// Since we provide getauxval ourselves (to minimize the version dependency on runtime glibc), we are the ones
    // being called and we fail horribly:
    ///
    ///    ==301455==ERROR: MemorySanitizer: SEGV on unknown address 0x2ffc6d721550 (pc 0x5622c1cc0073 bp 0x000000000003 sp 0x7ffc6d721530 T301455)
    ///    ==301455==The signal is caused by a WRITE memory access.
    /// #0 0x5622c1cc0073 in __auxv_init_procfs ./ClickHouse/base/glibc-compatibility/musl/getauxval.c:129:5
    /// #1 0x5622c1cbffe9 in getauxval ./ClickHouse/base/glibc-compatibility/musl/getauxval.c:240:12
    /// #2 0x5622c0d7bfb4 in __sanitizer::ReExec() crtstuff.c
    /// #3 0x5622c0df7bfc in __msan::InitShadowWithReExec(bool) crtstuff.c
    /// #4 0x5622c0d95356 in __msan_init (./ClickHouse/build_msan/contrib/google-protobuf-cmake/protoc+0x256356) (BuildId: 6411d3c88b898ba3f7d49760555977d3e61f0741)
    /// #5 0x5622c0dfe878 in msan.module_ctor main.cc
    /// #6 0x5622c1cc156c in __libc_csu_init (./ClickHouse/build_msan/contrib/google-protobuf-cmake/protoc+0x118256c) (BuildId: 6411d3c88b898ba3f7d49760555977d3e61f0741)
    /// #7 0x73dc05dd7ea3 in __libc_start_main /usr/src/debug/glibc/glibc/csu/../csu/libc-start.c:343:6
    /// #8 0x5622c0d6b7cd in _start (./ClickHouse/build_msan/contrib/google-protobuf-cmake/protoc+0x22c7cd) (BuildId: 6411d3c88b898ba3f7d49760555977d3e61f0741)

    /// The source of the issue above is that, at this point in time during __msan_init, we can't really do much as
    /// most global variables aren't initialized or available yet, so we can't initiate the auxiliary vector.
    /// Normal glibc / musl getauxval doesn't have this problem since they initiate their auxval vector at the very
    /// start of __libc_start_main (just keeping track of argv+argc+1), but we don't have such option (otherwise
    /// this complexity of reading "/proc/self/auxv" or using __environ would not be necessary).

    /// To avoid this crashes on the re-exec call (see above how it would fail when creating `aux`, and if we used
    /// __auxv_init_environ then it would SIGSEV on READing `__environ`) we capture this call for `AT_EXECFN` and
    /// unconditionally return "/proc/self/exe" without any preparation. Theoretically this should be fine in
    /// our case, as we don't load any libraries. That's the theory at least.
    if (type == AT_EXECFN)
        return (unsigned long)"/proc/self/exe";
#    endif
#endif

    // For debugging:
    // - od -t dL /proc/self/auxv
    // - LD_SHOW_AUX= ls
    int fd = open("/proc/self/auxv", O_RDONLY);
    // It is possible in case of:
    // - no procfs mounted
    // - on android you are not able to read it unless running from shell or debugging
    // - some other issues
    if (fd == -1)
    {
        // Fallback to environ.
        a_cas_p(&getauxval_func, (void *)__auxv_init_procfs, (void *)__auxv_init_environ);
        return __auxv_init_environ(type);
    }

    ElfW(auxv_t) aux;

    /// NOTE: sizeof(aux) is very small (less then PAGE_SIZE), so partial read should not be possible.
    _Static_assert(sizeof(aux) < 4096, "Unexpected sizeof(aux)");
    while (__retry_read(fd, &aux, sizeof(aux)) == sizeof(aux))
    {
#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
        __msan_unpoison(&aux, sizeof(aux));
#endif
#endif
        if (aux.a_type == AT_NULL)
        {
            break;
        }
        if (aux.a_type == AT_IGNORE || aux.a_type == AT_IGNOREPPC)
        {
            continue;
        }

        if (aux.a_type >= ARRAY_SIZE(__auxv_procfs))
        {
            fprintf(stderr, "AT_* is out of range: %li (maximum allowed is %zu)\n", aux.a_type, ARRAY_SIZE(__auxv_procfs));
            abort();
        }
        if (__auxv_procfs[aux.a_type])
        {
            /// It is possible due to race on initialization.
        }
        __auxv_procfs[aux.a_type] = aux.a_un.a_val;
    }
    close(fd);

    __auxv_secure = __getauxval_procfs(AT_SECURE);

    // Now we've initialized __auxv_procfs, next time getauxval() will only call __get_auxval().
    a_cas_p(&getauxval_func, (void *)__auxv_init_procfs, (void *)__getauxval_procfs);

    return __getauxval_procfs(type);
}

//
// auxv from environ interface
//
// NOTE: environ available only after static initializers,
// so you cannot rely on this if you need getauxval() before.
//
// Good example of such user is sanitizers, for example
// LSan will not work with __auxv_init_environ(),
// since it needs getauxval() before.
//
static size_t NO_SANITIZE_THREAD __find_auxv(unsigned long type)
{
    size_t i;
    for (i = 0; __auxv_environ[i]; i += 2)
    {
        if (__auxv_environ[i] == type)
        {
            return i + 1;
        }
    }
    return (size_t) -1;
}
unsigned long NO_SANITIZE_THREAD __getauxval_environ(unsigned long type)
{
    if (type == AT_SECURE)
        return __auxv_secure;

    if (__auxv_environ)
    {
        size_t index = __find_auxv(type);
        if (index != ((size_t) -1))
            return __auxv_environ[index];
    }

    errno = ENOENT;
    return 0;
}
static unsigned long NO_SANITIZE_THREAD __auxv_init_environ(unsigned long type)
{
    if (!__environ)
    {
        // __environ is not initialized yet so we can't initialize __auxv_environ right now.
        // That's normally occurred only when getauxval() is called from some sanitizer's internal code.
        errno = ENOENT;
        return 0;
    }

    // Initialize __auxv_environ and __auxv_secure.
    size_t i;
    for (i = 0; __environ[i]; i++);
    __auxv_environ = (unsigned long *) (__environ + i + 1);

    size_t secure_idx = __find_auxv(AT_SECURE);
    if (secure_idx != ((size_t) -1))
        __auxv_secure = __auxv_environ[secure_idx];

    // Now we need to switch to __getauxval_environ for all later calls, since
    // everything is initialized.
    a_cas_p(&getauxval_func, (void *)__auxv_init_environ, (void *)__getauxval_environ);

    return __getauxval_environ(type);
}

// Callchain:
// - __auxv_init_procfs -> __getauxval_environ
// - __auxv_init_procfs -> __auxv_init_environ -> __getauxval_environ
static void * volatile getauxval_func = (void *)__auxv_init_procfs;

unsigned long NO_SANITIZE_THREAD getauxval(unsigned long type)
{
    return ((unsigned long (*)(unsigned long))getauxval_func)(type);
}
