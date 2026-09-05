#pragma once

/// replxx itself is portable and ships a full Windows implementation (`src/windows.cxx` plus
/// the `_WIN32` branches in `src/terminal.cxx`). What is not portable is what ClickHouse's fork
/// added on top, in the commit that introduced support for custom descriptors:
///
///  - it writes to `_out_fd` with `dprintf`/`fsync` outside of any `#ifdef`, and mingw-w64 has
///    neither function;
///  - it removed the `replxx::tty::in`/`replxx::tty::out` globals in favour of a per-descriptor
///    `tty::is_a_tty( fd )`, but left `windows.cxx` referring to `tty::out`.
///
/// This header patches both up for the Windows build only. It is force-included into the replxx
/// translation units (see CMakeLists.txt) so that the vendored sources stay untouched. The
/// proper home for the fix is https://github.com/ClickHouse/replxx; until then this keeps
/// `clickhouse-client` on Windows from losing line editing altogether.

#include <io.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

namespace replxx
{
namespace tty
{

/// Defined in replxx-windows-tty.cxx, which also explains the semantics.
extern bool out;

}
}

/// `dprintf(3)`: formatted write straight to a descriptor, bypassing `FILE` buffering.
static inline int replxx_compat_dprintf(int fd, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    /// Ask for the required length first: replxx formats short control sequences and prompts,
    /// but nothing here bounds them, so do not guess a buffer size.
    va_list args_for_size;
    va_copy(args_for_size, args);
    const int length = vsnprintf(nullptr, 0, format, args_for_size);
    va_end(args_for_size);

    if (length < 0)
    {
        va_end(args);
        return length;
    }

    char * buffer = static_cast<char *>(malloc(static_cast<size_t>(length) + 1));
    if (!buffer)
    {
        va_end(args);
        return -1;
    }

    const int formatted = vsnprintf(buffer, static_cast<size_t>(length) + 1, format, args);
    va_end(args);

    int written = -1;
    if (formatted >= 0)
        written = _write(fd, buffer, static_cast<unsigned>(formatted));

    free(buffer);
    return written;
}

#define dprintf replxx_compat_dprintf

/// `fsync(2)`. `_write` above is a direct CRT descriptor write with no `FILE` buffer behind it,
/// so for the console this has nothing left to push and fails with `EBADF`; replxx ignores the
/// result. It still matters when the descriptor is a redirected file.
static inline int replxx_compat_fsync(int fd)
{
    return _commit(fd);
}

#define fsync replxx_compat_fsync
