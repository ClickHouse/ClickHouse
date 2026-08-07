#pragma once

#include <Common/RadixSort.h>

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
    using Result = size_t;

    static T & extractKey(Element & elem) { return elem.value; }
    static size_t extractResult(Element & elem) { return elem.index; }
};

}
