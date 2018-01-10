#pragma once

#include <Columns/IColumn.h>

namespace DB::GatherUtils
{

template <typename T>
struct NumericArraySlice
{
    const T * data;
    size_t size;
};

struct GenericArraySlice
{
    const IColumn * elements;
    size_t begin;
    size_t size;
};

template <typename Slice>
struct NullableArraySlice : public Slice
{
    const UInt8 * null_map = nullptr;

    NullableArraySlice() = default;
    NullableArraySlice(const Slice & base) : Slice(base) {}
};

template <typename T>
using NumericSlice = const T *;

}

