#pragma once

/// This header properly includes datasketches headers to avoid conflicts with murmurhash.
///
/// Problem: Both contrib/murmurhash and contrib/datasketches-cpp define MurmurHash3.h
/// but only the datasketches version defines the HashState type which is required by
/// the HLL (HyperLogLog) and other datasketches headers.
///
/// Solution: Explicitly include the datasketches version of MurmurHash3.h BEFORE
/// any datasketches headers that depend on it.
///
/// Usage: All ClickHouse code that uses datasketches should include this header
/// instead of including datasketches headers directly.

#if USE_DATASKETCHES

// Include the datasketches MurmurHash3.h explicitly BEFORE other datasketches headers.
// We must use a relative path because there are two MurmurHash3.h files:
//   1. contrib/murmurhash/include/MurmurHash3.h (doesn't define HashState)
//   2. contrib/datasketches-cpp/common/include/MurmurHash3.h (defines HashState)
//
// The datasketches version is marked as a SYSTEM include in CMakeLists.txt,
// which suppresses warnings. However, using a relative path here bypasses that.
// We use #pragma to suppress warnings for this specific third-party header.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wextra-semi-stmt"
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"

#if __has_include(<../../contrib/datasketches-cpp/common/include/MurmurHash3.h>)
    #include <../../contrib/datasketches-cpp/common/include/MurmurHash3.h>
#else
    #include <MurmurHash3.h>
#endif

#pragma clang diagnostic pop

// Now include other datasketches headers (these use SYSTEM includes and don't need pragmas)
#include <hll.hpp>
#include <quantiles_sketch.hpp>

#endif
