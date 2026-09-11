#pragma once

#include <cstddef>

namespace DB
{

class JemallocNoDumpArenas
{
public:
    /// Allocate memory that is excluded from core dumps (`MADV_DONTDUMP`), tracked by the memory tracker.
    /// Alignments up to `alignof(std::max_align_t)` take the default path. Throws `CANNOT_ALLOCATE_MEMORY` on failure.
    [[nodiscard]] static void * allocate(size_t bytes, size_t alignment = 0);

    /// Release memory obtained from `allocate`.
    static void deallocate(void * ptr) noexcept;
};

}
