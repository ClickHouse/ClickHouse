/// Startup code for AArch64 executables linked against glibc, used instead of `Scrt1.o`, `crti.o`,
/// `crtbeginS.o`, `crtendS.o`, `crtn.o` and `libc_nonshared.a` when the build enables branch protection
/// (see `ENABLE_BRANCH_PROTECTION`).
///
/// The startup objects of the sysroot predate BTI and lack the BTI property, which makes the linker
/// drop BTI for the whole executable. They can't be marked as is either: `ld.so` and `__libc_start_main`
/// enter them through indirect branches, which fault without a landing pad once BTI is enforced.
/// This file is compiled with `-mbranch-protection=standard`, so it carries the property and every
/// address-taken function in it starts with a landing pad.
///
/// Nothing uses the `.init`/`.fini` sections (`_init`/`_fini`), only `.init_array`/`.fini_array`.
/// For the main executable `ld.so` runs `.preinit_array` and `.fini_array`, and `__libc_start_main`
/// runs `.init_array` through the `init` argument (glibc before 2.34; newer glibc runs the same
/// callback for binaries linked against an older version of `__libc_start_main`).

#include <stddef.h>

int main(int argc, char ** argv, char ** envp);

int __libc_start_main(
    int (*main)(int, char **, char **),
    int argc,
    char ** argv,
    void (*init)(int, char **, char **),
    void (*fini)(void),
    void (*rtld_fini)(void),
    void * stack_end);

typedef void (*InitFunction)(int, char **, char **);

extern const InitFunction __init_array_start[] __attribute__((visibility("hidden")));
extern const InitFunction __init_array_end[] __attribute__((visibility("hidden")));

void __libc_csu_init(int argc, char ** argv, char ** envp)
{
    const size_t size = __init_array_end - __init_array_start;
    for (size_t i = 0; i < size; ++i)
        __init_array_start[i](argc, argv, envp);
}

void __libc_csu_fini(void)
{
}

/// Identifies the module in `__cxa_atexit` (normally defined by `crtbeginS.o`).
void * __dso_handle __attribute__((visibility("hidden"))) = &__dso_handle;

/// Tells glibc that the executable uses the current `libio` ABI (normally defined by `crt1.o`).
__attribute__((used)) const int _IO_stdin_used = 0x20001;

/// The rest of `libc_nonshared.a` of glibc 2.28: wrappers that pass `__dso_handle` or the version of
/// `struct stat` (0 on AArch64) to the functions of `libc.so`. This object precedes the library on the
/// link line, so the linker doesn't take its members, which lack the BTI property.
/// The `stat` structures are opaque here to avoid the inline definitions of `stat` in `<sys/stat.h>`.

int __cxa_atexit(void (*func)(void *), void * arg, void * dso_handle);
int __cxa_at_quick_exit(void (*func)(void), void * dso_handle);
int __register_atfork(void (*prepare)(void), void (*parent)(void), void (*child)(void), void * dso_handle);
void __stack_chk_fail(void) __attribute__((noreturn));

int __xstat(int version, const char * path, void * buf);
int __xstat64(int version, const char * path, void * buf);
int __fxstat(int version, int fd, void * buf);
int __fxstat64(int version, int fd, void * buf);
int __lxstat(int version, const char * path, void * buf);
int __lxstat64(int version, const char * path, void * buf);
int __fxstatat(int version, int dirfd, const char * path, void * buf, int flags);
int __fxstatat64(int version, int dirfd, const char * path, void * buf, int flags);
int __xmknod(int version, const char * path, unsigned int mode, unsigned long * dev);
int __xmknodat(int version, int dirfd, const char * path, unsigned int mode, unsigned long * dev);

#pragma GCC visibility push(hidden)

int atexit(void (*func)(void))
{
    return __cxa_atexit((void (*)(void *))func, NULL, __dso_handle);
}

int at_quick_exit(void (*func)(void))
{
    return __cxa_at_quick_exit(func, __dso_handle);
}

int pthread_atfork(void (*prepare)(void), void (*parent)(void), void (*child)(void))
{
    return __register_atfork(prepare, parent, child, __dso_handle);
}

int __pthread_atfork(void (*prepare)(void), void (*parent)(void), void (*child)(void)) __attribute__((alias("pthread_atfork")));

void __stack_chk_fail_local(void) __attribute__((noreturn));
void __stack_chk_fail_local(void)
{
    __stack_chk_fail();
}

int stat(const char * path, void * buf) { return __xstat(0, path, buf); }
int stat64(const char * path, void * buf) { return __xstat64(0, path, buf); }
int fstat(int fd, void * buf) { return __fxstat(0, fd, buf); }
int fstat64(int fd, void * buf) { return __fxstat64(0, fd, buf); }
int lstat(const char * path, void * buf) { return __lxstat(0, path, buf); }
int lstat64(const char * path, void * buf) { return __lxstat64(0, path, buf); }
int fstatat(int dirfd, const char * path, void * buf, int flags) { return __fxstatat(0, dirfd, path, buf, flags); }
int fstatat64(int dirfd, const char * path, void * buf, int flags) { return __fxstatat64(0, dirfd, path, buf, flags); }
int mknod(const char * path, unsigned int mode, unsigned long dev) { return __xmknod(0, path, mode, &dev); }
int mknodat(int dirfd, const char * path, unsigned int mode, unsigned long dev) { return __xmknodat(0, dirfd, path, mode, &dev); }

int __stat(const char * path, void * buf) __attribute__((alias("stat")));
int __fstat(int fd, void * buf) __attribute__((alias("fstat")));
int __lstat(const char * path, void * buf) __attribute__((alias("lstat")));
int __mknod(const char * path, unsigned int mode, unsigned long dev) __attribute__((alias("mknod")));

#pragma GCC visibility pop

/// The same entry point as `sysdeps/aarch64/start.S` of glibc. `ld.so` jumps to it with `x0` holding
/// the `rtld_fini` function and `sp` pointing to `argc`, followed by `argv`. It starts with `bti jc`
/// (`hint #38`), since `ld.so` may enter it with either `br` or `blr`.
__asm__(
    ".text\n"
    ".global _start\n"
    ".type _start, %function\n"
    "_start:\n"
    "    hint #38\n"
    "    mov x29, #0\n"
    "    mov x30, #0\n"
    "    mov x5, x0\n"
    "    ldr x1, [sp]\n"
    "    add x2, sp, #8\n"
    "    mov x6, sp\n"
    "    adrp x0, :got:main\n"
    "    ldr x0, [x0, :got_lo12:main]\n"
    "    adrp x3, __libc_csu_init\n"
    "    add x3, x3, :lo12:__libc_csu_init\n"
    "    adrp x4, __libc_csu_fini\n"
    "    add x4, x4, :lo12:__libc_csu_fini\n"
    "    bl __libc_start_main\n"
    "    bl abort\n"
    ".size _start, . - _start\n");
