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
concept has_find_extreme_implementation = (is_any_of<T, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64>);

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end);

template <has_find_extreme_implementation T>
std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end);

#define EXTERN_INSTANTIATION(T) \
    extern template std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMinNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMinIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMaxNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<T> findExtremeMaxIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    extern template std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end); \
    extern template std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end);

FOR_BASIC_NUMERIC_TYPES(EXTERN_INSTANTIATION)
#undef EXTERN_INSTANTIATION

}
