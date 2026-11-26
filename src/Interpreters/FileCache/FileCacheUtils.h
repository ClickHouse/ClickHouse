#pragma once

#include <cstddef>
#include <stdexcept>
#include <base/arithmeticOverflow.h>


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

    /// Round up using the remainder rather than the `num + multiple - 1` trick, which can
    /// falsely overflow even when the rounded-up value is representable (e.g. `num = 2`,
    /// `multiple = SIZE_MAX`, where the correct result `SIZE_MAX` fits but `num + multiple - 1`
    /// does not). We only signal overflow when the actual rounded-up value exceeds `SIZE_MAX`.
    const size_t remainder = num % multiple;
    if (remainder == 0)
        return num;

    size_t result = 0;
    if (common::addOverflow(num, multiple - remainder, result))
        throw std::overflow_error("FileCacheUtils::roundUpToMultiple: rounded-up value does not fit in size_t");

    return result;
}

}
