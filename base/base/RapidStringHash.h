#pragma once

/** The single entry point through which every string hash in ClickHouse is computed.
  *
  * Dispatch rule:
  *   size <  256 -> `rapid::rapidhash`, a scalar hash that runs everywhere;
  *   size >= 256 -> `taotie::hash64`, which needs AVX-512 and VAES and falls back to
  *                  `rapid::rapidhash` when they are not available at compile time.
  *
  * Callers:
  *   `StringViewHash` in `base/base/StringViewHash.h`
  *   `StringHashTableHash` in `src/Common/HashTable/StringHashTable.h`
  */

#include <base/RapidHash.h>
#include <base/TaotieHash.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

/// The hashes are never persisted, so the seed is a plain constant rather than a per-process random value.
inline constexpr uint64_t STRING_HASH_SEED = 0;

/// The size from which the vectorized hash pays off.
inline constexpr size_t STRING_HASH_VECTORIZED_THRESHOLD = 256;

inline size_t rapidStringHash(std::string_view key)
{
#if defined(__AVX512F__) && defined(__VAES__)
    if (key.size() >= STRING_HASH_VECTORIZED_THRESHOLD)
        return taotie::hash64(key.data(), key.size(), STRING_HASH_SEED);
#endif

    return rapid::rapidhash(key.data(), key.size(), STRING_HASH_SEED);
}
