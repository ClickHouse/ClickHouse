#pragma once

#include <DataTypes/IDataType.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Concepts.h>

#include <algorithm>
#include <optional>

namespace DB
{
template <typename T>
concept has_find_extreme_implementation
    = (is_any_of<T, Int8, Int16, Int32, Int64, Int128, Int256, UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Float32, Float64>);
template <typename T>
concept underlying_has_find_extreme_implementation = (is_any_of<T, Decimal32, Decimal64, Decimal128, Decimal256, DateTime64>);

/// `findExtreme*Index` scans twice, which only pays off when the first pass is vectorized.
/// The exclusion goes through `NativeType` so that it also catches `Decimal128` and `Decimal256`.
template <typename T>
concept has_find_extreme_index_implementation
    = ((has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
       && !is_big_int_v<NativeType<T>>);

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_index_implementation<T>)
std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end);

template <typename T>
requires(has_find_extreme_index_implementation<T>)
std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end);

#define EXTERN_INSTANTIATION_VALUE(T) \
    extern template std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMinNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMinIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMaxNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMaxIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

#define EXTERN_INSTANTIATION(T) \
    EXTERN_INSTANTIATION_VALUE(T) \
    extern template std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end); \
    extern template std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end);

FOR_BASIC_NUMERIC_TYPES(EXTERN_INSTANTIATION)

EXTERN_INSTANTIATION(Decimal32)
EXTERN_INSTANTIATION(Decimal64)
EXTERN_INSTANTIATION(DateTime64)

EXTERN_INSTANTIATION_VALUE(Int128)
EXTERN_INSTANTIATION_VALUE(Int256)
EXTERN_INSTANTIATION_VALUE(UInt128)
EXTERN_INSTANTIATION_VALUE(UInt256)
EXTERN_INSTANTIATION_VALUE(Decimal128)
EXTERN_INSTANTIATION_VALUE(Decimal256)
#undef EXTERN_INSTANTIATION
#undef EXTERN_INSTANTIATION_VALUE
}
