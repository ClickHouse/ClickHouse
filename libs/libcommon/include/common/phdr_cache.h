#pragma once

/// This code was developed by Fedor Korotkiy (prime@yandex-team.ru) for YT product in Yandex.

//! Collects all dl_phdr_info items and caches them in a static array.
//! Also rewrites dl_iterate_phdr with a lockless version which consults the above cache
//! thus eliminating scalability bottleneck in C++ exception unwinding.
//! As a drawback, this only works if no dynamic object loading/unloading happens after this point.
//! This function is not thread-safe; the caller must guarantee that no concurrent unwinding takes place.
void enablePHDRCache();
