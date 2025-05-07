#pragma once

#include <base/types.h>

#include <array>

namespace DB
{

template <typename T>
constexpr auto getBitmapSize()
{
    return
        (sizeof(T) == 32) ?
            4 :
        (sizeof(T) == 16) ?
            2 :
        ((sizeof(T) == 8) ?
            1 :
        ((sizeof(T) == 4) ?
            1 :
        ((sizeof(T) == 2) ?
            1 :
        0)));
}

template <typename T>
using KeysNullMap = std::array<UInt8, getBitmapSize<T>()>;

}
