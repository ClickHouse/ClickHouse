#pragma once

/// Exponential growth of the extents requested from the extent hooks when retaining virtual memory.
/// jemalloc: `exp_grow.h`, `src/exp_grow.c`.

#include <allocator/Common.h>
#include <allocator/SizeClasses.h>

namespace jemalloc
{

/// jemalloc: exp_grow_t
///
/// Protected by the owner's `grow_mtx`.
struct ExpGrow
{
    /// Next extent size class in a growing series to use when satisfying a request via the extent hooks (only if
    /// `opt_retain`). This limits the number of disjoint virtual memory ranges so that extent merging can be
    /// effective even if multiple arenas' extent allocation requests are highly interleaved.
    pszind_t next;
    /// The max allowed size index to expand (unless the required size is greater) (`retain_grow_limit`). Default is
    /// no limit, and controlled through mallctl only.
    pszind_t limit;

    /// Enforces a minimum of 2M grow, which is convenient for the huge page use cases. HUGEPAGE is not used as the
    /// value, because on some platforms it can be very large (e.g. 512M on aarch64 with 64K pages).
    /// jemalloc: exp_grow_init
    void init()
    {
        const size_t min_grow = size_t(2) << 20;
        next = sz::psz2ind(min_grow);
        limit = sz::psz2ind(SC_LARGE_MAXCLASS);
    }

    /// Computes the size to request from the hooks for an allocation of at least `alloc_size_min` bytes, and how many
    /// size classes of the series are skipped. Returns true on error (the size is outside the legal range).
    /// jemalloc: exp_grow_size_prepare
    JE_ALWAYS_INLINE bool sizePrepare(size_t alloc_size_min, size_t * r_alloc_size, pszind_t * r_skip) const
    {
        *r_skip = 0;
        *r_alloc_size = sz::pind2sz(next + *r_skip);
        while (*r_alloc_size < alloc_size_min)
        {
            (*r_skip)++;
            if (next + *r_skip >= sz::psz2ind(SC_LARGE_MAXCLASS))
            {
                /// Outside legal range.
                return true;
            }
            *r_alloc_size = sz::pind2sz(next + *r_skip);
        }
        return false;
    }

    /// jemalloc: exp_grow_size_commit
    JE_ALWAYS_INLINE void sizeCommit(pszind_t skip)
    {
        if (next + skip + 1 <= limit)
            next += skip + 1;
        else
            next = limit;
    }
};

static_assert(sizeof(ExpGrow) == 8);

}
