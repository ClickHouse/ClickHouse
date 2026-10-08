#pragma once

/// This code was based on the code by Fedor Korotkiy https://www.linkedin.com/in/fedor-korotkiy-659a1838/

/** Collects all dl_phdr_info items and caches them in a static array.
  * Also rewrites dl_iterate_phdr with a lock-free version which consults the above cache
  * thus eliminating scalability bottleneck in C++ exception unwinding.
  * As a drawback, this only works if no dynamic object unloading happens after this point.
  * This function is thread-safe. You should call it to update cache after loading new shared libraries.
  * Otherwise exception handling from dlopened libraries won't work (will call std::terminate immediately).
  * NOTE: dlopen is forbidden in our code.
  *
  * NOTE: It is disabled with Thread Sanitizer because TSan can only use original "dl_iterate_phdr" function.
  */
void updatePHDRCache();

/** Whether capturing a stack trace from within an async signal handler is safe on this
  * platform/build. This gates features that unwind in a signal handler (the Query Profiler)
  * or that depend on such a source (the TraceCollector).
  *
  * - Linux: true once `updatePHDRCache` has installed the lock-free `dl_iterate_phdr` (libunwind
  *   calls `dl_iterate_phdr` while unwinding `.eh_frame`, and glibc's takes a loader lock).
  * - musl: always true, its `dl_iterate_phdr` is inherently lock-free.
  * - macOS: always true, unwinding uses frame-pointer `backtrace` which never calls
  *   `dl_iterate_phdr`, so no cache is needed.
  */
bool hasAsyncSignalSafeUnwind();
