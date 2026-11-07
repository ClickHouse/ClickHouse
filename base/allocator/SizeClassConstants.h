#pragma once

/// Compile-time size class constants (jemalloc: the macros of `sc.h`).
///
/// Separated from `SizeClasses.h` so that `Bitmap.h` (which needs `SIZE_CLASS_LOG2_SLAB_MAX_REGIONS` and `SIZE_CLASS_NUM_SIZES`) can be
/// included
/// by `SizeClasses.h` (which needs `BitmapInfo` for `BinInfo`).
///
/// Size classes are of the form `(1 << log2_base) + (num_delta << log2_delta)`; groups of `SIZE_CLASS_GROUP_SIZE` classes share
/// `log2_base` and `log2_delta`, and the size doubles every `SIZE_CLASS_GROUP_SIZE` classes. See the comment at the top of `sc.h`.

#include <allocator/Common.h>

#include <limits>

namespace jemalloc
{

/// Size class N + (1 << SIZE_CLASS_LOG2_GROUP_SIZE) is twice the size of size class N.
inline constexpr int SIZE_CLASS_LOG2_GROUP_SIZE = 2;
inline constexpr int SIZE_CLASS_LOG2_TINY_MIN = 3;

static_assert(SIZE_CLASS_LOG2_TINY_MIN != 0, "The div module doesn't support division by 1");

inline constexpr size_t SIZE_CLASS_GROUP_SIZE = size_t(1) << SIZE_CLASS_LOG2_GROUP_SIZE;
inline constexpr unsigned SIZE_CLASS_PTR_BITS = (1u << LG_SIZEOF_PTR) * 8;
inline constexpr unsigned SIZE_CLASS_NUM_TINY = LOG2_QUANTUM - SIZE_CLASS_LOG2_TINY_MIN;
inline constexpr int SIZE_CLASS_LOG2_TINY_MAX_CLASS = int(LOG2_QUANTUM) > SIZE_CLASS_LOG2_TINY_MIN ? int(LOG2_QUANTUM) - 1 : -1;
inline constexpr unsigned SIZE_CLASS_NUM_PSEUDO = SIZE_CLASS_GROUP_SIZE;
inline constexpr unsigned SIZE_CLASS_LOG2_FIRST_REGULAR_BASE = LOG2_QUANTUM + SIZE_CLASS_LOG2_GROUP_SIZE;

/// Allocations are capped below 2 ** (ptr_bits - 1), so the highest base is 2 ** (ptr_bits - 2), and the last group
/// is one class shorter than the others.
inline constexpr unsigned SIZE_CLASS_LOG2_BASE_MAX = SIZE_CLASS_PTR_BITS - 2;
inline constexpr unsigned SIZE_CLASS_NUM_REGULAR
    = SIZE_CLASS_GROUP_SIZE * (SIZE_CLASS_LOG2_BASE_MAX - SIZE_CLASS_LOG2_FIRST_REGULAR_BASE + 1) - 1;
inline constexpr unsigned SIZE_CLASS_NUM_SIZES = SIZE_CLASS_NUM_TINY + SIZE_CLASS_NUM_PSEUDO + SIZE_CLASS_NUM_REGULAR;

/// The number of size classes that are a multiple of the page size.
inline constexpr unsigned SIZE_CLASS_NUM_PAGE_SIZES = SIZE_CLASS_GROUP_SIZE
    + (SIZE_CLASS_LOG2_BASE_MAX - (LOG2_PAGE + SIZE_CLASS_LOG2_GROUP_SIZE)) * SIZE_CLASS_GROUP_SIZE + SIZE_CLASS_GROUP_SIZE - 1;

/// A size class is binnable (small, slab-allocated) if size < page size * group.
inline constexpr unsigned SIZE_CLASS_NUM_BINS = SIZE_CLASS_NUM_TINY + SIZE_CLASS_NUM_PSEUDO
    + SIZE_CLASS_GROUP_SIZE * (LOG2_PAGE + SIZE_CLASS_LOG2_GROUP_SIZE - SIZE_CLASS_LOG2_FIRST_REGULAR_BASE) - 1;

/// The size2index table uses uint8_t to encode each bin index.
static_assert(SIZE_CLASS_NUM_BINS <= 256, "Too many small size classes");

/// The largest size class in the lookup table, and its binary log.
inline constexpr unsigned SIZE_CLASS_LOG2_MAX_LOOKUP = 12;
inline constexpr size_t SIZE_CLASS_LOOKUP_MAX_CLASS = size_t(1) << SIZE_CLASS_LOG2_MAX_LOOKUP;

/// Internal, only used for the definition of SIZE_CLASS_SMALL_MAX_CLASS.
inline constexpr size_t SIZE_CLASS_SMALL_MAX_BASE = size_t(1) << (LOG2_PAGE + SIZE_CLASS_LOG2_GROUP_SIZE - 1);
inline constexpr size_t SIZE_CLASS_SMALL_MAX_DELTA = size_t(1) << (LOG2_PAGE - 1);

/// The largest size class allocated out of a slab.
inline constexpr size_t SIZE_CLASS_SMALL_MAX_CLASS = SIZE_CLASS_SMALL_MAX_BASE + (SIZE_CLASS_GROUP_SIZE - 1) * SIZE_CLASS_SMALL_MAX_DELTA;

/// The fast path assumes all lookup-able sizes are small.
static_assert(SIZE_CLASS_SMALL_MAX_CLASS >= SIZE_CLASS_LOOKUP_MAX_CLASS, "Lookup table sizes must be small");

/// The smallest size class not allocated out of a slab.
inline constexpr size_t SIZE_CLASS_LARGE_MIN_CLASS = size_t(1) << (LOG2_PAGE + SIZE_CLASS_LOG2_GROUP_SIZE);
inline constexpr unsigned SIZE_CLASS_LOG2_LARGE_MIN_CLASS = LOG2_PAGE + SIZE_CLASS_LOG2_GROUP_SIZE;

/// Internal; only used for the definition of SIZE_CLASS_LARGE_MAX_CLASS.
inline constexpr size_t SIZE_CLASS_MAX_BASE = size_t(1) << (SIZE_CLASS_PTR_BITS - 2);
inline constexpr size_t SIZE_CLASS_MAX_DELTA = size_t(1) << (SIZE_CLASS_PTR_BITS - 2 - SIZE_CLASS_LOG2_GROUP_SIZE);

/// The largest size class supported.
inline constexpr size_t SIZE_CLASS_LARGE_MAX_CLASS = SIZE_CLASS_MAX_BASE + (SIZE_CLASS_GROUP_SIZE - 1) * SIZE_CLASS_MAX_DELTA;

/// The allocation fast path relies on it to subtract sizes from a ssize_t.
static_assert(SIZE_CLASS_LARGE_MAX_CLASS < size_t(std::numeric_limits<ssize_t>::max()));

/// Maximum number of regions in one slab (`CONFIG_LG_SLAB_MAXREGS` is never set by ClickHouse).
inline constexpr unsigned SIZE_CLASS_LOG2_SLAB_MAX_REGIONS = LOG2_PAGE - SIZE_CLASS_LOG2_TINY_MIN;
inline constexpr unsigned SIZE_CLASS_SLAB_MAX_REGIONS = 1u << SIZE_CLASS_LOG2_SLAB_MAX_REGIONS;

/// With large size classes disabled, the tcache still caches sizes up to this threshold (see `sc.h`).
inline constexpr unsigned LOG2_USABLE_SIZE_GROW_SLOW_THRESHOLD = SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_PAGE + 1;
inline constexpr unsigned USABLE_SIZE_GROW_SLOW_THRESHOLD = 1u << LOG2_USABLE_SIZE_GROW_SLOW_THRESHOLD;

}
