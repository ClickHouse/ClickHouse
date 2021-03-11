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
struct NullableSlice : public Slice
{
    const UInt8 * null_map = nullptr;

    NullableSlice() = default;
    NullableSlice(const Slice & base) : Slice(base) {}
};

template <typename T>
struct NumericValueSlice
{
    T value;
    static constexpr size_t size = 1;
};

struct GenericValueSlice
{
    const IColumn * elements;
    size_t position;
    static constexpr size_t size = 1;
};

}

