#pragma once

/// This code was based on the code by Fedor Korotkiy (prime@yandex-team.ru) for YT product in Yandex.

/** Collects all dl_phdr_info items and caches them in a static array.
  * Also rewrites dl_iterate_phdr with a lock-free version which consults the above cache
  * thus eliminating scalability bottleneck in C++ exception unwinding.
  * As a drawback, this only works if no dynamic object unloading happens after this point.
  * This function is thread-safe. You should call it to update cache after loading new shared libraries.
  * Otherwise exception handling from dlopened libraries won't work (will call std::terminate immediately).
  *
  * NOTE: It is disabled with Thread Sanitizer because TSan can only use original "dl_iterate_phdr" function.
  */
void updatePHDRCache();

/** Check if "dl_iterate_phdr" will be lock-free
  * to determine if some features like Query Profiler can be used.
  */
bool hasPHDRCache();
