#pragma once

/// Blocked bloom filter for runtime JOIN filters.
///
/// Three variant implementations are available for benchmarking:
///   BlockedBloomFilter8Hashes.h  — 8-hash salt-multiply, 32-byte blocks, AVX2 SIMD
///   BlockedBloomFilter4Hashes.h  — 4-hash salt-multiply, 16-byte blocks, SSE/NEON SIMD
///   BlockedBloomFilterArrow.h    — Arrow-style mask table, 8-byte blocks, scalar
///
/// To switch variants, change the include and the using alias below.

#include <Common/BlockedBloomFilter4Hashes.h>

namespace DB
{

using BlockedBloomFilter = BlockedBloomFilter4Hashes;
using BlockedBloomFilterPtr = std::unique_ptr<BlockedBloomFilter>;

}
