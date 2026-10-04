#pragma once

/// Compile-time size class constants (jemalloc: the macros of `sc.h`).
///
/// Separated from `SizeClasses.h` so that `Bitmap.h` (which needs `SC_LG_SLAB_MAXREGS` and `SC_NSIZES`) can be included
/// by `SizeClasses.h` (which needs `BitmapInfo` for `BinInfo`).
///
/// Size classes are of the form `(1 << lg_base) + (ndelta << lg_delta)`; groups of `SC_NGROUP` classes share
/// `lg_base` and `lg_delta`, and the size doubles every `SC_NGROUP` classes. See the comment at the top of `sc.h`.

#include <allocator/Common.h>

#include <limits>

namespace jemalloc
{

/// Size class N + (1 << SC_LG_NGROUP) is twice the size of size class N.
inline constexpr int SC_LG_NGROUP = 2;
inline constexpr int SC_LG_TINY_MIN = 3;

static_assert(SC_LG_TINY_MIN != 0, "The div module doesn't support division by 1");

inline constexpr size_t SC_NGROUP = size_t(1) << SC_LG_NGROUP;
inline constexpr unsigned SC_PTR_BITS = (1u << LG_SIZEOF_PTR) * 8;
inline constexpr unsigned SC_NTINY = LG_QUANTUM - SC_LG_TINY_MIN;
inline constexpr int SC_LG_TINY_MAXCLASS = int(LG_QUANTUM) > SC_LG_TINY_MIN ? int(LG_QUANTUM) - 1 : -1;
inline constexpr unsigned SC_NPSEUDO = SC_NGROUP;
inline constexpr unsigned SC_LG_FIRST_REGULAR_BASE = LG_QUANTUM + SC_LG_NGROUP;

/// Allocations are capped below 2 ** (ptr_bits - 1), so the highest base is 2 ** (ptr_bits - 2), and the last group
/// is one class shorter than the others.
inline constexpr unsigned SC_LG_BASE_MAX = SC_PTR_BITS - 2;
inline constexpr unsigned SC_NREGULAR = SC_NGROUP * (SC_LG_BASE_MAX - SC_LG_FIRST_REGULAR_BASE + 1) - 1;
inline constexpr unsigned SC_NSIZES = SC_NTINY + SC_NPSEUDO + SC_NREGULAR;

/// The number of size classes that are a multiple of the page size.
inline constexpr unsigned SC_NPSIZES
    = SC_NGROUP + (SC_LG_BASE_MAX - (LG_PAGE + SC_LG_NGROUP)) * SC_NGROUP + SC_NGROUP - 1;

/// A size class is binnable (small, slab-allocated) if size < page size * group.
inline constexpr unsigned SC_NBINS = SC_NTINY + SC_NPSEUDO + SC_NGROUP * (LG_PAGE + SC_LG_NGROUP - SC_LG_FIRST_REGULAR_BASE) - 1;

/// The size2index table uses uint8_t to encode each bin index.
static_assert(SC_NBINS <= 256, "Too many small size classes");

/// The largest size class in the lookup table, and its binary log.
inline constexpr unsigned SC_LG_MAX_LOOKUP = 12;
inline constexpr size_t SC_LOOKUP_MAXCLASS = size_t(1) << SC_LG_MAX_LOOKUP;

/// Internal, only used for the definition of SC_SMALL_MAXCLASS.
inline constexpr size_t SC_SMALL_MAX_BASE = size_t(1) << (LG_PAGE + SC_LG_NGROUP - 1);
inline constexpr size_t SC_SMALL_MAX_DELTA = size_t(1) << (LG_PAGE - 1);

/// The largest size class allocated out of a slab.
inline constexpr size_t SC_SMALL_MAXCLASS = SC_SMALL_MAX_BASE + (SC_NGROUP - 1) * SC_SMALL_MAX_DELTA;

/// The fast path assumes all lookup-able sizes are small.
static_assert(SC_SMALL_MAXCLASS >= SC_LOOKUP_MAXCLASS, "Lookup table sizes must be small");

/// The smallest size class not allocated out of a slab.
inline constexpr size_t SC_LARGE_MINCLASS = size_t(1) << (LG_PAGE + SC_LG_NGROUP);
inline constexpr unsigned SC_LG_LARGE_MINCLASS = LG_PAGE + SC_LG_NGROUP;

/// Internal; only used for the definition of SC_LARGE_MAXCLASS.
inline constexpr size_t SC_MAX_BASE = size_t(1) << (SC_PTR_BITS - 2);
inline constexpr size_t SC_MAX_DELTA = size_t(1) << (SC_PTR_BITS - 2 - SC_LG_NGROUP);

/// The largest size class supported.
inline constexpr size_t SC_LARGE_MAXCLASS = SC_MAX_BASE + (SC_NGROUP - 1) * SC_MAX_DELTA;

/// The allocation fast path relies on it to subtract sizes from a ssize_t.
static_assert(SC_LARGE_MAXCLASS < size_t(std::numeric_limits<ssize_t>::max()));

/// Maximum number of regions in one slab (`CONFIG_LG_SLAB_MAXREGS` is never set by ClickHouse).
inline constexpr unsigned SC_LG_SLAB_MAXREGS = LG_PAGE - SC_LG_TINY_MIN;
inline constexpr unsigned SC_SLAB_MAXREGS = 1u << SC_LG_SLAB_MAXREGS;

/// With large size classes disabled, the tcache still caches sizes up to this threshold (see `sc.h`).
inline constexpr unsigned LG_USIZE_GROW_SLOW_THRESHOLD = SC_LG_NGROUP + LG_PAGE + 1;
inline constexpr unsigned USIZE_GROW_SLOW_THRESHOLD = 1u << LG_USIZE_GROW_SLOW_THRESHOLD;

}
