#pragma once

#include <cstddef>
#include <new>

namespace DB
{

class JemallocNoDumpArenas
{
public:
    /// Allocate memory that is excluded from core dumps (`MADV_DONTDUMP`), tracked by the memory tracker.
    /// Alignments up to `alignof(std::max_align_t)` take the default path. Throws `CANNOT_ALLOCATE_MEMORY` on failure.
    [[nodiscard]] static void * allocate(size_t bytes, size_t alignment = 0);

    /// Same as `allocate` for C allocators, which cannot handle exceptions: the allocation is accounted like a C `malloc`, so the
    /// memory limit is enforced by the next throwing allocation instead of here, and a failed allocation returns `nullptr`.
    [[nodiscard]] static void * allocate(size_t bytes, size_t alignment, std::nothrow_t) noexcept;

    /// Release memory obtained from `allocate`.
    static void deallocate(void * ptr) noexcept;
};

}
