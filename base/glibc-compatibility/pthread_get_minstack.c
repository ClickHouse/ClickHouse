/// Rust std looks up this internal glibc function with `dlsym` and calls it on every thread spawn.
/// A call through a pointer into glibc fails the CFI check, so with CFI the binary exports its own definition,
/// which `dlsym` finds first (see `ENABLE_CFI` in CMakeLists.txt). https://github.com/rust-lang/rust/issues/166068

#define _GNU_SOURCE
#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

/// The loader's static TLS size, including the surplus and the TCB, which glibc takes from the thread stack.
/// `GLIBC_PRIVATE`, so looked up as compiler-rt does: a direct reference fails the compatibility check.
/// The call goes into the loader, which has no CFI type, so it is not checked.
__attribute__((no_sanitize("cfi-icall"))) static size_t staticTLSSize(void)
{
    void (*get_tls_static_info)(size_t *, size_t *) = (void (*)(size_t *, size_t *))dlsym(RTLD_DEFAULT, "_dl_get_tls_static_info");
    if (!get_tls_static_info)
        abort();
    size_t size = 0;
    size_t align = 0;
    get_tls_static_info(&size, &align);
    return (size + align - 1) / align * align;
}

/// As in glibc: a page, the static TLS and `PTHREAD_STACK_MIN`.
size_t __pthread_get_minstack(const pthread_attr_t * attr)
{
    (void)attr;
    return (size_t)sysconf(_SC_PAGESIZE) + staticTLSSize() + PTHREAD_STACK_MIN;
}

/// CFI gives a function a jump table entry, which `dlsym` must return, only if its address is taken in the program.
__attribute__((used)) static size_t (* const pthread_get_minstack_address)(const pthread_attr_t *) = __pthread_get_minstack;
