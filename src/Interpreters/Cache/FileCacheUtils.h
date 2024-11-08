#pragma once
#include <Core/Types.h>

namespace FileCacheUtils
{

static size_t roundDownToMultiple(size_t num, size_t multiple)
{
    return (num / multiple) * multiple;
}

static size_t roundUpToMultiple(size_t num, size_t multiple)
{
    return roundDownToMultiple(num + multiple - 1, multiple);
}

}
