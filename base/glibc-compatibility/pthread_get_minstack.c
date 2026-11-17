/// Rust std looks up this internal glibc function with `dlsym` and calls it on every thread spawn.
/// A call through a pointer into glibc fails the CFI check, so with CFI the binary exports its own definition,
/// which `dlsym` finds first (see `ENABLE_CFI` in CMakeLists.txt). https://github.com/rust-lang/rust/issues/166068

#define _GNU_SOURCE
#include <limits.h>
#include <link.h>
#include <pthread.h>
#include <unistd.h>

static int addTLSSize(struct dl_phdr_info * info, size_t size, void * data)
{
    (void)size;
    size_t * tls_size = data;
    for (size_t i = 0; i < info->dlpi_phnum; ++i)
    {
        if (info->dlpi_phdr[i].p_type == PT_TLS)
            *tls_size += info->dlpi_phdr[i].p_memsz + info->dlpi_phdr[i].p_align;
    }
    return 0;
}

/// As in glibc: a page, the static TLS, which glibc takes from the thread stack (unlike musl), and `PTHREAD_STACK_MIN`.
size_t __pthread_get_minstack(const pthread_attr_t * attr)
{
    (void)attr;
    size_t tls_size = 0;
    dl_iterate_phdr(addTLSSize, &tls_size);
    return (size_t)sysconf(_SC_PAGESIZE) + tls_size + PTHREAD_STACK_MIN;
}

/// CFI gives a function a jump table entry, which `dlsym` must return, only if its address is taken in the program.
__attribute__((used)) static size_t (* const pthread_get_minstack_address)(const pthread_attr_t *) = __pthread_get_minstack;
