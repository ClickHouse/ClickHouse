#pragma once

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

    /// Check for potential overflow when num + multiple - 1 > SIZE_MAX.
    /// In this case, round down to the nearest multiple without overflow.
    if (const size_t limit = SIZE_MAX - multiple + 1; num > limit)
        return roundDownToMultiple(SIZE_MAX, multiple);

    return roundDownToMultiple(num + multiple - 1, multiple);
}

}
