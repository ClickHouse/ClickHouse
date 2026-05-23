#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace FileCacheUtils
{

inline size_t roundDownToMultiple(size_t num, size_t multiple)
{
    if (!multiple)
        return num;
    return (num / multiple) * multiple;
}

inline size_t roundUpToMultiple(size_t num, size_t multiple)
{
    if (!multiple)
        return num;

    const size_t rem = num % multiple;
    if (rem == 0)
        return num;

    /// The smallest multiple of `multiple` that is `>= num` equals `num + delta`. We compute
    /// `delta` from the remainder so that representable results (e.g. `multiple = SIZE_MAX`,
    /// `num = 2` → `SIZE_MAX`) are not incorrectly rejected by the `num + multiple - 1`
    /// overflow trick. Callers rely on the post-condition `result >= num`, so throw rather
    /// than silently returning a smaller value when the rounded-up multiple itself does not
    /// fit in size_t. `FileCacheSettings::validate` already rejects absurdly large
    /// `boundary_alignment` values, so this branch is defensive and should not be reachable
    /// on valid configurations.
    const size_t delta = multiple - rem;
    if (num > SIZE_MAX - delta)
        throw std::overflow_error("FileCacheUtils::roundUpToMultiple: rounded-up value does not fit in size_t");

    return num + delta;
}

}
