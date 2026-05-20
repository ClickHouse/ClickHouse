#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace FileCacheUtils
{

static size_t roundDownToMultiple(size_t num, size_t multiple)
{
    if (!multiple)
        return num;
    return (num / multiple) * multiple;
}

static size_t roundUpToMultiple(size_t num, size_t multiple)
{
    if (!multiple)
        return num;

    /// The rounded-up value would not fit in size_t: there is no representable multiple of
    /// `multiple` greater than or equal to `num`. Callers rely on the post-condition
    /// `result >= num`, so silently returning a smaller value would corrupt size arithmetic.
    /// `FileCacheSettings::validate` already rejects absurdly large `boundary_alignment` values,
    /// so this branch is defensive and should not be reachable on valid configurations.
    if (const size_t limit = SIZE_MAX - multiple + 1; num > limit)
        throw std::overflow_error("FileCacheUtils::roundUpToMultiple: rounded-up value does not fit in size_t");

    return roundDownToMultiple(num + multiple - 1, multiple);
}

}
