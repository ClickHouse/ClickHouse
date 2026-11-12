#pragma once

/* librseq discovers the libc rseq ABI exclusively through
 * dlsym(RTLD_NEXT, "__rseq_offset" / "__rseq_size" / "__rseq_flags"), which
 * cannot resolve anything in a statically linked binary (there is no dynamic
 * symbol table; musl's static dlsym reports "Symbol not found"). Redirect the
 * lookup to weak link-time references instead: they bind to the libc's
 * definitions when the ABI is provided (our musl) and stay null otherwise,
 * preserving librseq's RSEQ_INIT_ERROR_MISSING_SYMBOLS error path.
 *
 * Force-included (-include) only for librseq's own translation units; rseq.c
 * is the sole dlsym caller. dlfcn.h must be included before the macro is
 * defined, or the macro would mangle the declaration of dlsym itself.
 *
 * The clean equivalent (a dlsym-then-weak-fallback inside rseq_init) is a
 * candidate for upstream compudj/librseq; drop this shim if that lands. */

#include <dlfcn.h>
#include <stddef.h>

extern const ptrdiff_t __rseq_offset __attribute__((weak));
extern const unsigned int __rseq_size __attribute__((weak));
extern const unsigned int __rseq_flags __attribute__((weak));

static inline void * clickhouse_rseq_static_dlsym(const char * name)
{
    if (!__builtin_strcmp(name, "__rseq_offset"))
        return (void *)&__rseq_offset;
    if (!__builtin_strcmp(name, "__rseq_size"))
        return (void *)&__rseq_size;
    if (!__builtin_strcmp(name, "__rseq_flags"))
        return (void *)&__rseq_flags;
    return 0;
}

#define dlsym(handle, name) ((void)(handle), clickhouse_rseq_static_dlsym(name))
