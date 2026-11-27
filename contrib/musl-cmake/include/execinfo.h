/* Stub execinfo.h for musl libc.
 *
 * musl does not provide execinfo.h / backtrace(). ClickHouse uses
 * unw_backtrace (libunwind) for stack traces and LLVM has backtraces
 * disabled (ENABLE_BACKTRACES=0), so these are no-op stubs that only
 * satisfy the #include and symbol references.
 */

#ifndef _EXECINFO_H_
#define _EXECINFO_H_

#ifdef __cplusplus
extern "C" {
#endif

static inline int backtrace(void **buffer, int size)
{
    (void)buffer;
    (void)size;
    return 0;
}

static inline char **backtrace_symbols(void *const *buffer, int size)
{
    (void)buffer;
    (void)size;
    return 0;
}

static inline void backtrace_symbols_fd(void *const *buffer, int size, int fd)
{
    (void)buffer;
    (void)size;
    (void)fd;
}

#ifdef __cplusplus
}
#endif

#endif /* _EXECINFO_H_ */
