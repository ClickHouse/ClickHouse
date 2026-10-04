#pragma once

/// Message output and allocation-free formatting.
/// jemalloc: `malloc_io.h`, `src/malloc_io.c`.
///
/// `formatV` supports exactly the subset of `snprintf(3)` that `malloc_vsnprintf` supports (no floating point):
/// flags `#`, `-`, ` `, `+`; width (digits, a leading `0` turns on zero padding, or `*`); precision (only used by `%s`);
/// length modifiers `l`, `ll`, `q`, `j`, `t`, `z`; conversions `%`, `d`, `i`, `o`, `u`, `x`, `X`, `c`, `s`, `p`.
/// The quirks of the original are preserved (see the comments in Format.cpp).

#include <allocator/Common.h>

#include <cinttypes>
#include <cstdarg>
#include <cstdint>
#include <sys/types.h>

/// The application-settable message hook (`je_malloc_message`), read on every call.
/// NOTE: defined (weak, default visibility) in Format.cpp for now; it may move to Api.cpp.
extern "C" __attribute__((visibility("default"))) void (*je_malloc_message)(void * cbopaque, const char * s);

/// Format macros for 32/64-bit integers and pointers (`FMTd64` etc. in jemalloc).
#define FMTd32 PRId32
#define FMTu32 PRIu32
#define FMTx32 PRIx32
#define FMTd64 PRId64
#define FMTu64 PRIu64
#define FMTx64 PRIx64
#define FMTdPTR PRIdPTR
#define FMTuPTR PRIuPTR
#define FMTxPTR PRIxPTR

namespace jemalloc
{

/// `write_cb_t`: receives a NUL-terminated string.
using WriteCallback = void(void * cbopaque, const char * s);

/// Size of a stack-allocated buffer passed to `bufferError` (`BUFERROR_BUF`).
inline constexpr size_t BUFERROR_BUF = 64;

/// Size of the stack buffer used by `printToCallbackV` and friends (`MALLOC_PRINTF_BUFSIZE`).
inline constexpr size_t MALLOC_PRINTF_BUFSIZE = 4096;

/// The default message writer: writes `s` to stderr.
/// jemalloc: wrtmessage
void defaultWriteMessage(void * cbopaque, const char * s);

/// `je_malloc_message` if set, otherwise `defaultWriteMessage`.
/// jemalloc: `je_malloc_message != NULL ? je_malloc_message : wrtmessage` (in `malloc_vcprintf`, `buf_writer_init`)
inline WriteCallback * messageCallback()
{
    return je_malloc_message != nullptr ? je_malloc_message : defaultWriteMessage;
}

/// Write a string through `je_malloc_message` (or to stderr if it is not set).
/// jemalloc: malloc_write
void writeMessage(const char * s);

/// `strerror_r` wrapper that always fills `buf`. Returns 0 on success (or the XSI `strerror_r` result).
/// jemalloc: buferror
int bufferError(int err, char * buf, size_t buflen);

/// `strtoumax` clone with jemalloc's exact semantics, including `errno` (EINVAL / ERANGE) and the quirk that a
/// leading `0` followed by a non-octal, non-`x` character stops the conversion right after the `0`.
/// jemalloc: malloc_strtoumax
uintmax_t strToUMax(const char * nptr, const char ** endptr, int base);

inline uintmax_t strToUMax(const char * nptr, char ** endptr, int base)
{
    return strToUMax(nptr, const_cast<const char **>(endptr), base);
}

/// Returns the length of the would-be output (excluding the NUL); writes at most `size` bytes and always
/// NUL-terminates. `size` must be non-zero.
/// jemalloc: malloc_vsnprintf
size_t formatV(char * str, size_t size, const char * fmt, va_list ap);

/// jemalloc: malloc_snprintf
size_t format(char * str, size_t size, const char * fmt, ...) JE_FORMAT_PRINTF(3, 4);

/// Format into a `MALLOC_PRINTF_BUFSIZE` stack buffer (truncating) and pass it to `write_cb` once;
/// `write_cb == nullptr` means `messageCallback()`.
/// jemalloc: malloc_vcprintf
void printToCallbackV(WriteCallback * write_cb, void * cbopaque, const char * fmt, va_list ap);

/// jemalloc: malloc_cprintf
void printToCallback(WriteCallback * write_cb, void * cbopaque, const char * fmt, ...) JE_FORMAT_PRINTF(3, 4);

/// Print through `je_malloc_message` (or to stderr).
/// jemalloc: malloc_printf
void printMessage(const char * fmt, ...) JE_FORMAT_PRINTF(1, 2);

/// Write all `count` bytes, retrying on EINTR. Returns the number of bytes written or a negative value on error.
/// jemalloc: malloc_write_fd
ssize_t writeFd(int fd, const void * buf, size_t count);

/// Read up to `count` bytes (until EOF), retrying on EINTR. Returns the number of bytes read or a negative value.
/// jemalloc: malloc_read_fd
ssize_t readFd(int fd, void * buf, size_t count);

/// jemalloc: malloc_open
int openFile(const char * path, int flags);

/// jemalloc: malloc_close
int closeFile(int fd);

/// jemalloc: malloc_lseek
off_t seekFile(int fd, off_t offset, int whence);

}
