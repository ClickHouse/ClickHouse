# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

# Wire compiler-rt runtimes (builtins/sanitizers/XRay) into the link flags.
include (cmake/compiler_rt_link.cmake)

# The mingw-w64 CRT, in the order the mingw link line requires: `mingw32` holds the
# startup glue that calls `main`, `mingwex` the libc functions that `msvcrt` itself does
# not export (most of `<math.h>`, `*at` file calls, ...), and `msvcrt` forwards to the
# CRT DLL shipped with Windows. `winpthread` is mingw-w64's pthread implementation - we
# link it rather than using the Win32 threading API directly because ClickHouse (and
# libc++, see `cmake/cxx.cmake`) uses pthread interfaces throughout.
#
# The Win32 libraries after them are the import libraries for the OS DLLs we call:
# sockets (`ws2_32`), console and file APIs (`kernel32`), security tokens and the
# registry (`advapi32`), the user's profile directory (`shell32`, `ole32`), symbol
# lookup for stack traces (`dbghelp`), process memory statistics (`psapi`) and the
# system CSPRNG (`bcrypt`).
set (DEFAULT_LIBS "${DEFAULT_LIBS} -lmingw32 -lmingwex -lmsvcrt -lwinpthread")
set (DEFAULT_LIBS "${DEFAULT_LIBS} -lws2_32 -lkernel32 -ladvapi32 -lshell32 -lole32 -luserenv -ldbghelp -lpsapi -lbcrypt -luser32 -lntdll")

# Windows takes the default thread stack size from the PE header instead of from a
# process-wide limit like `RLIMIT_STACK`, and the default is only 1 MiB - too little for our
# recursive-descent parser and for analyzer passes that recurse on deeply-nested join trees.
# Raise it to the 8 MiB that is customary on Linux. This covers every thread, not only the
# main one: `CreateThread`/`_beginthreadex` and `pthread_create` all fall back to the PE
# header value when no explicit size is given, which is why - unlike on macOS, see
# `src/Common/ThreadStackSize.h` - no per-thread override is needed here.
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--stack,8388608")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES winpthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

# `base/harmful` interposes libc functions that must never be called. It is not built
# here: it relies on ELF symbol interposition, which has no PE equivalent.
