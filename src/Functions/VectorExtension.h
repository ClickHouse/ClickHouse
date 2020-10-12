#pragma once

#include <common/types.h>
// Contains types declarations and wrappers for GCC vector extension.

namespace DB::VectorExtension
{

typedef UInt64 UInt64x2  __attribute__ ((vector_size (sizeof(UInt64) * 2)));
typedef UInt64 UInt64x4  __attribute__ ((vector_size (sizeof(UInt64) * 4)));
typedef UInt64 UInt64x8  __attribute__ ((vector_size (sizeof(UInt64) * 8)));

typedef UInt32 UInt32x2  __attribute__ ((vector_size (sizeof(UInt32) * 2)));
typedef UInt32 UInt32x4  __attribute__ ((vector_size (sizeof(UInt32) * 4)));
typedef UInt32 UInt32x8  __attribute__ ((vector_size (sizeof(UInt32) * 8)));
typedef UInt32 UInt32x16 __attribute__ ((vector_size (sizeof(UInt32) * 16)));

typedef UInt16 UInt16x2  __attribute__ ((vector_size (sizeof(UInt16) * 2)));
typedef UInt16 UInt16x4  __attribute__ ((vector_size (sizeof(UInt16) * 4)));
typedef UInt16 UInt16x8  __attribute__ ((vector_size (sizeof(UInt16) * 8)));
typedef UInt16 UInt16x16 __attribute__ ((vector_size (sizeof(UInt16) * 16)));
typedef UInt16 UInt16x32 __attribute__ ((vector_size (sizeof(UInt16) * 32)));

typedef UInt8 UInt8x2   __attribute__ ((vector_size (sizeof(UInt8) * 2)));
typedef UInt8 UInt8x4   __attribute__ ((vector_size (sizeof(UInt8) * 4)));
typedef UInt8 UInt8x8   __attribute__ ((vector_size (sizeof(UInt8) * 8)));
typedef UInt8 UInt8x16  __attribute__ ((vector_size (sizeof(UInt8) * 16)));
typedef UInt8 UInt8x32  __attribute__ ((vector_size (sizeof(UInt8) * 32)));
typedef UInt8 UInt8x64  __attribute__ ((vector_size (sizeof(UInt8) * 64)));

namespace detail
{
    template <int Size>
    struct DummyStruct;

    template <>
    struct DummyStruct<4>
    {
        using UInt8Type = UInt8x4;
        using UInt16Type = UInt16x4;
        using UInt32Type = UInt32x4;
        using UInt64Type = UInt64x4;
    };
    template <>
    struct DummyStruct<8>
    {
        using UInt8Type = UInt8x8;
        using UInt16Type = UInt16x8;
        using UInt32Type = UInt32x8;
        using UInt64Type = UInt64x8;
    };
    template <>
    struct DummyStruct<16>
    {
        using UInt8Type = UInt8x16;
        using UInt16Type = UInt16x16;
        using UInt32Type = UInt32x16;
    };
    template <>
    struct DummyStruct<32>
    {
        using UInt8Type = UInt8x32;
        using UInt16Type = UInt16x32;
    };

}

// Same as above via template, e.g. UInt64x<8>
template <int Size>
using UInt8x = typename detail::DummyStruct<Size>::UInt8Type;
template <int Size>
using UInt16x = typename detail::DummyStruct<Size>::UInt16Type;
template <int Size>
using UInt32x = typename detail::DummyStruct<Size>::UInt32Type;
template <int Size>
using UInt64x = typename detail::DummyStruct<Size>::UInt64Type;

}
