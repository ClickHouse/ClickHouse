#pragma once

#include <base/types.h>

namespace DB
{

inline Int64 floorDivide(Int64 numerator, Int64 denominator)
{
    Int64 quotient = numerator / denominator;
    if (numerator % denominator != 0 && (numerator < 0) != (denominator < 0))
        --quotient;
    return quotient;
}

}
