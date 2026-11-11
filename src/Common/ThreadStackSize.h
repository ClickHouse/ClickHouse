#pragma once

#include <cstddef>

namespace DB
{

/// Desired stack size (in bytes) for the worker threads we create ourselves.
///
/// macOS gives secondary threads only a 512 KiB stack by default (the main thread gets 8 MiB),
/// and there is no process-wide knob to change that default - unlike Linux, where the default
/// follows `RLIMIT_STACK` (normally 8 MiB). 512 KiB is too small for some deep call stacks, most
/// notably recursive query analysis that triggers LLVM JIT codegen: codegen is stack-hungry and
/// does not cooperate with `checkStackSize`, so it walks straight off the end into the guard page.
///
/// To avoid this, on Darwin we explicitly request an 8 MiB stack, matching the usual Linux default.
/// A value of 0 means "use the OS default" and is used on every other platform.
#if defined(OS_DARWIN)
inline constexpr size_t DEFAULT_THREAD_STACK_SIZE = 8 * 1024 * 1024;
#else
inline constexpr size_t DEFAULT_THREAD_STACK_SIZE = 0;
#endif

}
