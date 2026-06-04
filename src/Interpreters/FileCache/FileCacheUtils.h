#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <common/arithmeticOverflow.h>


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

    size_t upper_bound = num;
    if (common::addOverflow(upper_bound, multiple - 1))
        throw std::overflow_error("FileCacheUtils::roundUpToMultiple: rounded-up value does not fit in size_t");

    return roundDownToMultiple(upper_bound, multiple);
}

}
