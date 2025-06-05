#pragma once

#include <Common/RadixSort.h>
#include "base/types.h"

namespace DB
{

template <typename T>
struct ValueWithIndex
{
    T value;
    UInt32 index;
};

template <typename T>
struct RadixSortTraits : RadixSortNumTraits<T>
{
    using Element = ValueWithIndex<T>;
    using Result = UInt32;

    static T & extractKey(Element & elem) { return elem.value; }
    static size_t extractResult(Element & elem) { return elem.index; }
};

}
