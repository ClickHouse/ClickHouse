#pragma once

#include <base/types.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include "Sources.h"
#include "Sinks.h"
#include <Core/AccurateComparison.h>
#include <base/range.h>
#include "GatherUtils.h"

#if defined(__AVX2__) || defined(__SSE4_2__)
#include <immintrin.h>
#endif


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace DB::GatherUtils
{

inline constexpr size_t MAX_ARRAY_SIZE = 1 << 30;


/// Methods to copy Slice to Sink, overloaded for various combinations of types.

template <typename T>
void writeSlice(const NumericArraySlice<T> & slice, NumericArraySink<T> & sink)
{
    sink.elements.resize(sink.current_offset + slice.size);
    memcpySmallAllowReadWriteOverflow15(&sink.elements[sink.current_offset], slice.data, slice.size * sizeof(T));
    sink.current_offset += slice.size;
}

template <typename T, typename U>
void writeSlice(const NumericArraySlice<T> & slice, NumericArraySink<U> & sink)
{
    using NativeU = NativeType<U>;

    sink.elements.resize(sink.current_offset + slice.size);
    for (size_t i = 0; i < slice.size; ++i)
    {
        const auto & src = slice.data[i];
        auto & dst = sink.elements[sink.current_offset];

        if constexpr (is_over_big_int<T> || is_over_big_int<U>)
        {
            if constexpr (is_decimal<T>)
                dst = static_cast<NativeU>(src.value);
            else
                dst = static_cast<NativeU>(src);
        }
        else
            dst = static_cast<NativeU>(src);

        ++sink.current_offset;
    }
}

inline ALWAYS_INLINE void writeSlice(const StringSource::Slice & slice, StringSink & sink)
{
    sink.elements.resize(sink.current_offset + slice.size);
    memcpySmallAllowReadWriteOverflow15(&sink.elements[sink.current_offset], slice.data, slice.size);
    sink.current_offset += slice.size;
}

inline ALWAYS_INLINE void writeSlice(const StringSource::Slice & slice, FixedStringSink & sink)
{
    memcpySmallAllowReadWriteOverflow15(&sink.elements[sink.current_offset], slice.data, slice.size);
}

/// Assuming same types of underlying columns for slice and sink if (ArraySlice, ArraySink) is (GenericArraySlice, GenericArraySink).
inline ALWAYS_INLINE void writeSlice(const GenericArraySlice & slice, GenericArraySink & sink)
{
    if (slice.elements->structureEquals(sink.elements))
    {
        sink.elements.insertRangeFrom(*slice.elements, slice.begin, slice.size);
        sink.current_offset += slice.size;
    }
    else
        throw Exception("Function writeSlice expects same column types for GenericArraySlice and GenericArraySink.",
                        ErrorCodes::LOGICAL_ERROR);
}

template <typename T>
inline ALWAYS_INLINE void writeSlice(const GenericArraySlice & slice, NumericArraySink<T> & sink)
{
    sink.elements.resize(sink.current_offset + slice.size);
    for (size_t i = 0; i < slice.size; ++i)
    {
        Field field;
        slice.elements->get(slice.begin + i, field);
        sink.elements.push_back(applyVisitor(FieldVisitorConvertToNumber<T>(), field));
    }
    sink.current_offset += slice.size;
}

template <typename T>
inline ALWAYS_INLINE void writeSlice(const NumericArraySlice<T> & slice, GenericArraySink & sink)
{
    for (size_t i = 0; i < slice.size; ++i)
    {
        if constexpr (is_decimal<T>)
        {
            DecimalField field(T(slice.data[i]), 0); /// TODO: Decimal scale
            sink.elements.insert(field);
        }
        else
        {
            Field field = T(slice.data[i]);
            sink.elements.insert(field);
        }
    }
    sink.current_offset += slice.size;
}

template <typename Slice, typename ArraySink>
inline ALWAYS_INLINE void writeSlice(const NullableSlice<Slice> & slice, NullableArraySink<ArraySink> & sink)
{
    sink.null_map.resize(sink.current_offset + slice.size);

    if (slice.size == 1) /// Always true for ValueSlice.
        sink.null_map[sink.current_offset] = *slice.null_map;
    else
        memcpySmallAllowReadWriteOverflow15(&sink.null_map[sink.current_offset], slice.null_map, slice.size * sizeof(UInt8));

    writeSlice(static_cast<const Slice &>(slice), static_cast<ArraySink &>(sink));
}

template <typename Slice, typename ArraySink>
inline ALWAYS_INLINE void writeSlice(const Slice & slice, NullableArraySink<ArraySink> & sink)
{
    sink.null_map.resize(sink.current_offset + slice.size);

    if (slice.size == 1) /// Always true for ValueSlice.
        sink.null_map[sink.current_offset] = 0;
    else if (slice.size)
        memset(&sink.null_map[sink.current_offset], 0, slice.size * sizeof(UInt8));

    writeSlice(slice, static_cast<ArraySink &>(sink));
}


template <typename T, typename U>
void writeSlice(const NumericValueSlice<T> & slice, NumericArraySink<U> & sink)
{
    sink.elements.resize(sink.current_offset + 1);
    sink.elements[sink.current_offset] = slice.value;
    ++sink.current_offset;
}

/// Assuming same types of underlying columns for slice and sink if (ArraySlice, ArraySink) is (GenericValueSlice, GenericArraySink).
inline ALWAYS_INLINE void writeSlice(const GenericValueSlice & slice, GenericArraySink & sink)
{
    if (slice.elements->structureEquals(sink.elements))
    {
        sink.elements.insertFrom(*slice.elements, slice.position);
        ++sink.current_offset;
    }
    else
        throw Exception("Function writeSlice expects same column types for GenericValueSlice and GenericArraySink.",
                        ErrorCodes::LOGICAL_ERROR);
}

template <typename T>
inline ALWAYS_INLINE void writeSlice(const GenericValueSlice & slice, NumericArraySink<T> & sink)
{
    sink.elements.resize(sink.current_offset + 1);

    Field field;
    slice.elements->get(slice.position, field);
    sink.elements.push_back(applyVisitor(FieldVisitorConvertToNumber<T>(), field));
    ++sink.current_offset;
}

template <typename T>
inline ALWAYS_INLINE void writeSlice(const NumericValueSlice<T> & slice, GenericArraySink & sink)
{
    Field field = T(slice.value);
    sink.elements.insert(field);
    ++sink.current_offset;
}


template <typename SourceA, typename SourceB, typename Sink>
void NO_INLINE concat(SourceA && src_a, SourceB && src_b, Sink && sink)
{
    sink.reserve(src_a.getSizeForReserve() + src_b.getSizeForReserve());

    while (!src_a.isEnd())
    {
        writeSlice(src_a.getWhole(), sink);
        writeSlice(src_b.getWhole(), sink);

        sink.next();
        src_a.next();
        src_b.next();
    }
}

template <typename Source, typename Sink>
void concat(const std::vector<std::unique_ptr<IArraySource>> & array_sources, Sink && sink)
{
    size_t sources_num = array_sources.size();
    std::vector<char> is_const(sources_num);

    auto checkAndGetSizeToReserve = [] (auto source, IArraySource * array_source)
    {
        if (source == nullptr)
            throw Exception("Concat function expected " + demangle(typeid(Source).name()) + " or "
                            + demangle(typeid(ConstSource<Source>).name()) + " but got "
                            + demangle(typeid(*array_source).name()), ErrorCodes::LOGICAL_ERROR);
        return source->getSizeForReserve();
    };

    size_t size_to_reserve = 0;
    for (auto i : collections::range(0, sources_num))
    {
        auto & source = array_sources[i];
        is_const[i] = source->isConst();
        if (is_const[i])
            size_to_reserve += checkAndGetSizeToReserve(typeid_cast<ConstSource<Source> *>(source.get()), source.get());
        else
            size_to_reserve += checkAndGetSizeToReserve(typeid_cast<Source *>(source.get()), source.get());
    }

    sink.reserve(size_to_reserve);

    auto writeNext = [& sink] (auto source)
    {
        writeSlice(source->getWhole(), sink);
        source->next();
    };

    while (!sink.isEnd())
    {
        for (auto i : collections::range(0, sources_num))
        {
            auto & source = array_sources[i];
            if (is_const[i])
                writeNext(static_cast<ConstSource<Source> *>(source.get()));
            else
                writeNext(static_cast<Source *>(source.get()));
        }
        sink.next();
    }
}

template <typename Sink>
void NO_INLINE concat(StringSources & sources, Sink && sink)
{
    while (!sink.isEnd())
    {
        for (auto & source : sources)
        {
            writeSlice(source->getWhole(), sink);
            source->next();
        }
        sink.next();
    }
}


template <typename Source, typename Sink>
void NO_INLINE sliceFromLeftConstantOffsetUnbounded(Source && src, Sink && sink, size_t offset)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromLeft(offset), sink);
        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void NO_INLINE sliceFromLeftConstantOffsetBounded(Source && src, Sink && sink, size_t offset, ssize_t length)
{
    while (!src.isEnd())
    {
        ssize_t size = length;
        if (size < 0)
            size += static_cast<ssize_t>(src.getElementSize()) - offset;

        if (size > 0)
            writeSlice(src.getSliceFromLeft(offset, size), sink);

        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void NO_INLINE sliceFromRightConstantOffsetUnbounded(Source && src, Sink && sink, size_t offset)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromRight(offset), sink);
        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void NO_INLINE sliceFromRightConstantOffsetBounded(Source && src, Sink && sink, size_t offset, ssize_t length)
{
    while (!src.isEnd())
    {
        ssize_t size = length;
        if (size < 0)
            size += static_cast<ssize_t>(src.getElementSize()) - offset;

        if (size > 0)
            writeSlice(src.getSliceFromRight(offset, size), sink);

        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void NO_INLINE sliceDynamicOffsetUnbounded(Source && src, Sink && sink, const IColumn & offset_column)
{
    const bool is_null = offset_column.onlyNull();
    const auto * nullable = typeid_cast<const ColumnNullable *>(&offset_column);
    const ColumnUInt8::Container * null_map = nullable ? &nullable->getNullMapData() : nullptr;
    const IColumn * nested_column = nullable ? &nullable->getNestedColumn() : &offset_column;

    while (!src.isEnd())
    {
        auto row_num = src.rowNum();
        bool has_offset = !is_null && !(null_map && (*null_map)[row_num]);
        Int64 offset = has_offset ? nested_column->getInt(row_num) : 1;

        if (offset != 0)
        {
            typename std::decay_t<Source>::Slice slice;

            if (offset > 0)
                slice = src.getSliceFromLeft(offset - 1);
            else
                slice = src.getSliceFromRight(-static_cast<UInt64>(offset));

            writeSlice(slice, sink);
        }

        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void NO_INLINE sliceDynamicOffsetBounded(Source && src, Sink && sink, const IColumn & offset_column, const IColumn & length_column)
{
    const bool is_offset_null = offset_column.onlyNull();
    const auto * offset_nullable = typeid_cast<const ColumnNullable *>(&offset_column);
    const ColumnUInt8::Container * offset_null_map = offset_nullable ? &offset_nullable->getNullMapData() : nullptr;
    const IColumn * offset_nested_column = offset_nullable ? &offset_nullable->getNestedColumn() : &offset_column;

    const bool is_length_null = length_column.onlyNull();
    const auto * length_nullable = typeid_cast<const ColumnNullable *>(&length_column);
    const ColumnUInt8::Container * length_null_map = length_nullable ? &length_nullable->getNullMapData() : nullptr;
    const IColumn * length_nested_column = length_nullable ? &length_nullable->getNestedColumn() : &length_column;

    while (!src.isEnd())
    {
        size_t row_num = src.rowNum();
        bool has_offset = !is_offset_null && !(offset_null_map && (*offset_null_map)[row_num]);
        bool has_length = !is_length_null && !(length_null_map && (*length_null_map)[row_num]);
        Int64 offset = has_offset ? offset_nested_column->getInt(row_num) : 1;
        Int64 size = has_length ? length_nested_column->getInt(row_num) : static_cast<Int64>(src.getElementSize());

        if (size < 0)
            size += offset > 0 ? static_cast<Int64>(src.getElementSize()) - (offset - 1) : -UInt64(offset);

        if (offset != 0 && size > 0)
        {
            typename std::decay_t<Source>::Slice slice;

            if (offset > 0)
                slice = src.getSliceFromLeft(offset - 1, size);
            else
                slice = src.getSliceFromRight(-UInt64(offset), size);

            writeSlice(slice, sink);
        }

        sink.next();
        src.next();
    }
}


template <typename SourceA, typename SourceB, typename Sink>
void NO_INLINE conditional(SourceA && src_a, SourceB && src_b, Sink && sink, const PaddedPODArray<UInt8> & condition)
{
    sink.reserve(std::max(src_a.getSizeForReserve(), src_b.getSizeForReserve()));

    const UInt8 * cond_pos = condition.data();
    const UInt8 * cond_end = cond_pos + condition.size();

    bool a_is_short = src_a.getColumnSize() < condition.size();
    bool b_is_short = src_b.getColumnSize() < condition.size();

    while (cond_pos < cond_end)
    {
        if (*cond_pos)
            writeSlice(src_a.getWhole(), sink);
        else
            writeSlice(src_b.getWhole(), sink);

        if (!a_is_short || *cond_pos)
            src_a.next();
        if (!b_is_short || !*cond_pos)
            src_b.next();

        ++cond_pos;
        sink.next();
    }
}


template <typename T, typename U>
bool sliceEqualElements(const NumericArraySlice<T> & first [[maybe_unused]],
                        const NumericArraySlice<U> & second [[maybe_unused]],
                        size_t first_ind [[maybe_unused]],
                        size_t second_ind [[maybe_unused]])
{
    /// TODO: Decimal scale
    if constexpr (is_decimal<T> && is_decimal<U>)
        return accurate::equalsOp(first.data[first_ind].value, second.data[second_ind].value);
    else if constexpr (is_decimal<T> || is_decimal<U>)
        return false;
    else
        return accurate::equalsOp(first.data[first_ind], second.data[second_ind]);
}

template <typename T>
bool sliceEqualElements(const NumericArraySlice<T> &, const GenericArraySlice &, size_t, size_t)
{
    return false;
}

template <typename U>
bool sliceEqualElements(const GenericArraySlice &, const NumericArraySlice<U> &, size_t, size_t)
{
    return false;
}

inline ALWAYS_INLINE bool sliceEqualElements(const GenericArraySlice & first, const GenericArraySlice & second, size_t first_ind, size_t second_ind)
{
    return first.elements->compareAt(first_ind + first.begin, second_ind + second.begin, *second.elements, -1) == 0;
}

template <typename T>
bool insliceEqualElements(const NumericArraySlice<T> & first [[maybe_unused]],
                          size_t first_ind [[maybe_unused]],
                          size_t second_ind [[maybe_unused]])
{
    if constexpr (is_decimal<T>)
        return accurate::equalsOp(first.data[first_ind].value, first.data[second_ind].value);
    else
        return accurate::equalsOp(first.data[first_ind], first.data[second_ind]);
}
inline ALWAYS_INLINE bool insliceEqualElements(const GenericArraySlice & first, size_t first_ind, size_t second_ind)
{
    return first.elements->compareAt(first_ind + first.begin, second_ind + first.begin, *first.elements, -1) == 0;
}


/// For details of Knuth-Morris-Pratt string matching algorithm see
/// https://en.wikipedia.org/wiki/Knuth%E2%80%93Morris%E2%80%93Pratt_algorithm.
/// A "prefix-function" is defined as: i-th element is the length of the longest of all prefixes that end in i-th position
template <typename SliceType, typename EqualityFunc>
std::vector<size_t> buildKMPPrefixFunction(const SliceType & pattern, const EqualityFunc & isEqualFunc)
{
    std::vector<size_t> result(pattern.size);
    result[0] = 0;

    for (size_t i = 1; i < pattern.size; ++i)
    {
        result[i] = 0;
        for (size_t length = i; length > 0;)
        {
            length = result[length - 1];
            if (isEqualFunc(pattern, i, length))
            {
                result[i] = length + 1;
                break;
            }
        }
    }

    return result;
}


inline ALWAYS_INLINE bool hasNull(const UInt8 * null_map, size_t null_map_size)
{
    if (null_map != nullptr)
    {
        for (size_t i = 0; i < null_map_size; ++i)
        {
            if (null_map[i])
                return true;
        }
    }
    return false;
}


/// Methods to check if first array has elements from second array, overloaded for various combinations of types.
template <
    ArraySearchType search_type,
    typename FirstSliceType,
    typename SecondSliceType,
          bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t)>
bool sliceHasImplAnyAll(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    const bool has_second_null = hasNull(second_null_map, second.size);
    if (has_second_null)
    {
        const bool has_first_null = hasNull(first_null_map, first.size);

        if (has_first_null && search_type == ArraySearchType::Any)
            return true;

        if (!has_first_null && search_type == ArraySearchType::All)
            return false;
    }

    for (size_t i = 0; i < second.size; ++i)
    {
        if (has_second_null_map && second_null_map[i])
            continue;

        bool has = false;

        for (size_t j = 0; j < first.size && !has; ++j)
        {
            if (has_first_null_map && first_null_map[j])
                continue;

            if (isEqual(first, second, j, i))
            {
                has = true;
                break;
            }
        }

        if (has && search_type == ArraySearchType::Any)
            return true;

        if (!has && search_type == ArraySearchType::All)
            return false;
    }
    return search_type == ArraySearchType::All;
}


#if defined(__AVX2__) || defined(__SSE4_2__)

template<class T>
inline ALWAYS_INLINE bool hasAllIntegralLoopRemainder(
    size_t j, const NumericArraySlice<T> & first, const NumericArraySlice<T> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    for (; j < second.size; ++j)
    {
        // skip null elements since both have at least one - assuming it was checked earlier that at least one element in 'first' is null
        if (has_second_null_map && second_null_map[j])
            continue;

        bool found = false;

        for (size_t i = 0; i < first.size; ++i)
        {
            if (has_first_null_map && first_null_map[i])
                continue;

            if (first.data[i] == second.data[j])
            {
                found = true;
                break;
            }
        }

        if (!found)
            return false;
    }
    return true;
}

#endif


#if defined(__AVX2__)
// AVX2 Int specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<int>, NumericArraySlice<int>, sliceEqualElements<int,int> >(
    const NumericArraySlice<int> & first, const NumericArraySlice<int> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const int full = -1, none = 0;
    const __m256i ones = _mm256_set1_epi32(full);
    const __m256i zeros = _mm256_setzero_si256();
    if (second.size > 7 && first.size > 7)
    {
        for (; j < second.size - 7 && has_mask; j += 8)
        {
            has_mask = 0;
            const __m256i f_data = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(second.data + j));
            // bitmask is filled with minus ones for ones which are considered as null in the corresponding null map, 0 otherwise;
            __m256i bitmask = has_second_null_map ?
                _mm256_set_epi32(
                    (second_null_map[j + 7]) ? full : none,
                    (second_null_map[j + 6]) ? full : none,
                    (second_null_map[j + 5]) ? full : none,
                    (second_null_map[j + 4]) ? full : none,
                    (second_null_map[j + 3]) ? full : none,
                    (second_null_map[j + 2]) ? full : none,
                    (second_null_map[j + 1]) ? full : none,
                    (second_null_map[j]) ? full : none)
                : zeros;

            size_t i = 0;
            // Search first array to try to match all second elements
            for (; i < first.size - 7 && !has_mask; has_mask = _mm256_testc_si256(bitmask, ones), i += 8)
            {
                const __m256i s_data = _mm256_lddqu_si256(reinterpret_cast<const __m256i *>(first.data + i));
                // Create a mask to avoid to compare null elements
                // set_m128i takes two arguments: (high segment, low segment) that are two __m128i convert from 8bits to 32bits to fit to our following operations
                const __m256i first_nm_mask = _mm256_set_m128i(
                    _mm_cvtepi8_epi32(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map + i + 4))),
                    _mm_cvtepi8_epi32(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map + i))));
                bitmask =
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        first_nm_mask,
                                        _mm256_cmpeq_epi32(f_data, s_data)),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(6,5,4,3,2,1,0,7)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(6,5,4,3,2,1,0,7))))),
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(5,4,3,2,1,0,7,6)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(5,4,3,2,1,0,7,6)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(4,3,2,1,0,7,6,5)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(4,3,2,1,0,7,6,5)))))
                            ),
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(3,2,1,0,7,6,5,4)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(3,2,1,0,7,6,5,4)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(2,1,0,7,6,5,4,3)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(2,1,0,7,6,5,4,3))))),
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(1,0,7,6,5,4,3,2)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(1,0,7,6,5,4,3,2)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(0,7,6,5,4,3,2,1)),
                                        _mm256_cmpeq_epi32(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(0,7,6,5,4,3,2,1))))))),
                        bitmask);
            }

            if (i < first.size)
            {
                //  Loop(i)-jam
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;
                    __m256i v_i = _mm256_set1_epi32(first.data[i]);
                    bitmask = _mm256_or_si256(bitmask, _mm256_cmpeq_epi32(f_data, v_i));
                    has_mask = _mm256_testc_si256(bitmask, ones);
                }
            }
        }
    }

    if (!has_mask)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// TODO: Discuss about
// raise an error : "error: no viable conversion from 'const NumericArraySlice<unsigned int>' to 'const NumericArraySlice<int>'"
// How should we do, copy past each function ?? I haven't found a way to specialize a same function body for two different types.
// AVX2 UInt specialization
// template <>
// inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<unsigned>, NumericArraySlice<unsigned>, sliceEqualElements<unsigned,unsigned> >(
//     const NumericArraySlice<unsigned> & second, const NumericArraySlice<unsigned> & first, const UInt8 * first_null_map, const UInt8 * second_null_map)
// {
//     return sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<int>, NumericArraySlice<int>, sliceEqualElements<int,int> > (
//         static_cast<const NumericArraySlice<int> &>(second), static_cast<const NumericArraySlice<int> &>(first), second_null_map, first_null_map);
// }

// AVX2 Int64 specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<Int64>, NumericArraySlice<Int64>, sliceEqualElements<Int64,Int64> >(
    const NumericArraySlice<Int64> & first, const NumericArraySlice<Int64> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const int full = -1, none = 0;
    const __m256i ones = _mm256_set1_epi64x(full);
    const __m256i zeros = _mm256_setzero_si256();
    if (second.size > 3 && first.size > 3)
    {
        for (; j < second.size - 3 && has_mask; j += 4)
        {
            has_mask = 0;
            const __m256i f_data = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(second.data + j));
            __m256i bitmask = has_second_null_map ?
                _mm256_set_epi64x(
                    (second_null_map[j + 3])? full : none,
                    (second_null_map[j + 2])? full : none,
                    (second_null_map[j + 1])? full : none,
                    (second_null_map[j]) ? full : none)
                : zeros;

            unsigned i = 0;
            for (; i < first.size - 3 && !has_mask; has_mask = _mm256_testc_si256(bitmask, ones), i += 4)
            {
                const __m256i s_data = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(first.data + i));
                const __m256i first_nm_mask = _mm256_set_m128i(
                    _mm_cvtepi8_epi64(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map + i + 2))),
                    _mm_cvtepi8_epi64(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map + i))));
                bitmask =
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        first_nm_mask,
                                        _mm256_cmpeq_epi64(f_data, s_data)),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(5,4,3,2,1,0,7,6)),
                                        _mm256_cmpeq_epi64(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(5,4,3,2,1,0,7,6))))),

                            _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(3,2,1,0,7,6,5,4)),
                                        _mm256_cmpeq_epi64(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(3,2,1,0,7,6,5,4)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(1,0,7,6,5,4,3,2)),
                                        _mm256_cmpeq_epi64(f_data, _mm256_permutevar8x32_epi32(s_data, _mm256_set_epi32(1,0,7,6,5,4,3,2)))))),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;
                    __m256i v_i = _mm256_set1_epi64x(first.data[i]);
                    bitmask = _mm256_or_si256(bitmask, _mm256_cmpeq_epi64(f_data, v_i));
                    has_mask = _mm256_testc_si256(bitmask, ones);
                }
            }
        }
    }

    if (!has_mask && second.size > 2)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// AVX2 Int16_t specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<int16_t>, NumericArraySlice<int16_t>, sliceEqualElements<int16_t,int16_t> >(
    const NumericArraySlice<int16_t> & first, const NumericArraySlice<int16_t> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const int full = -1, none = 0;
    const __m256i ones = _mm256_set1_epi16(full);
    const __m256i zeros = _mm256_setzero_si256();
    if (second.size > 15 && first.size > 15)
    {
        for (; j < second.size - 15 && has_mask; j += 16)
        {
            has_mask = 0;
            const __m256i f_data = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(second.data + j));
            __m256i bitmask = has_second_null_map ?
                _mm256_set_epi16(
                    (second_null_map[j + 15]) ? full : none, (second_null_map[j + 14]) ? full : none,
                    (second_null_map[j + 13]) ? full : none, (second_null_map[j + 12]) ? full : none,
                    (second_null_map[j + 11]) ? full : none, (second_null_map[j + 10]) ? full : none,
                    (second_null_map[j + 9]) ? full : none, (second_null_map[j + 8])? full : none,
                    (second_null_map[j + 7]) ? full : none, (second_null_map[j + 6])? full : none,
                    (second_null_map[j + 5]) ? full : none, (second_null_map[j + 4])? full : none,
                    (second_null_map[j + 3]) ? full : none, (second_null_map[j + 2])? full : none,
                    (second_null_map[j + 1]) ? full : none, (second_null_map[j]) ? full : none)
                : zeros;
            unsigned i = 0;
            for (; i < first.size - 15 && !has_mask; has_mask = _mm256_testc_si256(bitmask, ones), i += 16)
            {
                const __m256i s_data = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(first.data + i));
                const __m256i first_nm_mask = _mm256_set_m128i(
                    _mm_cvtepi8_epi16(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map+i+8))),
                    _mm_cvtepi8_epi16(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map+i))));
                bitmask =
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            first_nm_mask,
                                            _mm256_cmpeq_epi16(f_data, s_data)),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26)))))
                                ),
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(s_data, _mm256_set_epi8(17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18))))))
                            ),
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_permute2x128_si256(first_nm_mask, first_nm_mask,1),
                                            _mm256_cmpeq_epi16(f_data, _mm256_permute2x128_si256(s_data, s_data, 1))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data, s_data, 1), _mm256_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data, s_data, 1), _mm256_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data, s_data, 1), _mm256_set_epi8(9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10)))))
                                ),
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data ,s_data, 1), _mm256_set_epi8(7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data, s_data, 1), _mm256_set_epi8(5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data ,s_data ,1), _mm256_set_epi8(3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2)),
                                            _mm256_cmpeq_epi16(f_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(s_data, s_data, 1), _mm256_set_epi8(1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2))))))
                        )
                    ),
                    bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;
                    __m256i v_i = _mm256_set1_epi16(first.data[i]);
                    bitmask = _mm256_or_si256(bitmask, _mm256_cmpeq_epi16(f_data, v_i));
                    has_mask = _mm256_testc_si256(bitmask, ones);
                }
            }
        }
    }

    if (!has_mask && second.size > 2)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

#elif defined(__SSE4_2__)

// SSE4.2 Int specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<int>, NumericArraySlice<int>, sliceEqualElements<int,int> >(
    const NumericArraySlice<int> & first, const NumericArraySlice<int> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const __m128i zeros = _mm_setzero_si128();
    if (second.size > 3 && first.size > 2)
    {
        const int full = -1, none = 0;
        for (; j < second.size - 3 && has_mask; j += 4)
        {
            has_mask = 0;
            const __m128i f_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(second.data + j));
            __m128i bitmask = has_second_null_map ?
                _mm_set_epi32(
                    (second_null_map[j + 3]) ? full : none,
                    (second_null_map[j + 2]) ? full : none,
                    (second_null_map[j + 1]) ? full : none,
                    (second_null_map[j]) ? full : none)
                : zeros;

            unsigned i = 0;
            for (; i < first.size - 3 && !has_mask; has_mask = _mm_test_all_ones(bitmask), i += 4)
            {
                const __m128i s_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(first.data + i));
                const __m128i first_nm_mask = has_first_null_map ?
                    _mm_cvtepi8_epi32(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map + i)))
                    : zeros;

                bitmask =
                    _mm_or_si128(
                        _mm_or_si128(
                            _mm_or_si128(
                                _mm_andnot_si128(
                                        first_nm_mask,
                                        _mm_cmpeq_epi32(f_data, s_data)),
                                _mm_andnot_si128(
                                    _mm_shuffle_epi32(first_nm_mask, _MM_SHUFFLE(2,1,0,3)),
                                    _mm_cmpeq_epi32(f_data, _mm_shuffle_epi32(s_data, _MM_SHUFFLE(2,1,0,3))))),
                            _mm_or_si128(
                                _mm_andnot_si128(
                                    _mm_shuffle_epi32(first_nm_mask, _MM_SHUFFLE(1,0,3,2)),
                                    _mm_cmpeq_epi32(f_data, _mm_shuffle_epi32(s_data, _MM_SHUFFLE(1,0,3,2)))),
                                _mm_andnot_si128(
                                    _mm_shuffle_epi32(first_nm_mask, _MM_SHUFFLE(0,3,2,1)),
                                    _mm_cmpeq_epi32(f_data, _mm_shuffle_epi32(s_data, _MM_SHUFFLE(0,3,2,1)))))
                        ),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;
                    __m128i r_i = _mm_set1_epi32(first.data[i]);
                    bitmask = _mm_or_si128(bitmask, _mm_cmpeq_epi32(f_data, r_i));
                    has_mask = _mm_test_all_ones(bitmask);
                }
            }
        }
    }

    if (!has_mask)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// SSE4.2 Int64 specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<Int64>, NumericArraySlice<Int64>, sliceEqualElements<Int64,Int64> >(
    const NumericArraySlice<Int64> & first, const NumericArraySlice<Int64> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const Int64 full = -1, none = 0;
    const __m128i zeros = _mm_setzero_si128();
    for (; j < second.size - 1 && has_mask; j += 2)
    {
        has_mask = 0;
        const __m128i f_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(second.data + j));
        __m128i bitmask = has_second_null_map ?
            _mm_set_epi64x(
                (second_null_map[j + 1]) ? full : none,
                (second_null_map[j]) ? full : none)
            : zeros;
        unsigned i = 0;
        for (; i < first.size - 1 && !has_mask; has_mask = _mm_test_all_ones(bitmask), i += 2)
        {
            const __m128i s_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(first.data + i));
            const __m128i first_nm_mask = has_first_null_map ?
                _mm_cvtepi8_epi64(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map + i)))
                : zeros;
            bitmask =
                _mm_or_si128(
                        _mm_or_si128(
                            _mm_andnot_si128(
                                first_nm_mask,
                                _mm_cmpeq_epi32(f_data, s_data)),
                            _mm_andnot_si128(
                                _mm_shuffle_epi32(first_nm_mask, _MM_SHUFFLE(1,0,3,2)),
                                _mm_cmpeq_epi64(f_data, _mm_shuffle_epi32(s_data, _MM_SHUFFLE(1,0,3,2))))),
                    bitmask);
        }

        if (i < first.size)
        {
            for (; i < first.size && !has_mask; ++i)
            {
                if (has_first_null_map && first_null_map[i])
                    continue;
                __m128i v_i = _mm_set1_epi64x(first.data[i]);
                bitmask = _mm_or_si128(bitmask, _mm_cmpeq_epi64(f_data, v_i));
                has_mask = _mm_test_all_ones(bitmask);
            }
        }
    }

    if (!has_mask)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// SSE4.2 Int16_t specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<int16_t>, NumericArraySlice<int16_t>, sliceEqualElements<int16_t,int16_t> >(
    const NumericArraySlice<int16_t> & first, const NumericArraySlice<int16_t> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const int16_t full = -1, none = 0;
    const __m128i zeros = _mm_setzero_si128();
    if (second.size > 6 && first.size > 6)
    {
        for (; j < second.size - 7 && has_mask; j += 8)
        {
            has_mask = 0;
            const __m128i f_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(second.data+j));
            __m128i bitmask = has_second_null_map ?
                _mm_set_epi16(
                    (second_null_map[j + 7]) ? full : none, (second_null_map[j + 6]) ? full : none,
                    (second_null_map[j + 5]) ? full : none, (second_null_map[j + 4]) ? full : none,
                    (second_null_map[j + 3]) ? full : none, (second_null_map[j + 2]) ? full : none,
                    (second_null_map[j + 1]) ? full : none, (second_null_map[j]) ? full: none)
                : zeros;
            unsigned i = 0;
            for (; i < first.size-7 && !has_mask; has_mask = _mm_test_all_ones(bitmask), i += 8)
            {
                const __m128i s_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(first.data + i));
                const __m128i first_nm_mask = has_first_null_map ?
                    _mm_cvtepi8_epi16(_mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map+i)))
                    : zeros;
                bitmask =
                    _mm_or_si128(
                            _mm_or_si128(
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            first_nm_mask,
                                            _mm_cmpeq_epi16(f_data, s_data)),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,15,14)),
                                            _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,15,14))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,15,14,13,12)),
                                            _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,15,14,13,12)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(9,8,7,6,5,4,3,2,1,0,15,14,13,12,11,10)),
                                            _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(9,8,7,6,5,4,3,2,1,0,15,14,13,12,11,10)))))
                                ),
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(7,6,5,4,3,2,1,0,15,14,13,12,11,10,9,8)),
                                            _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(7,6,5,4,3,2,1,0,15,14,13,12,11,10,9,8)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(5,4,3,2,1,0,15,14,13,12,11,10,9,8,7,6)),
                                        _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(5,4,3,2,1,0,15,14,13,12,11,10,9,8,7,6))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(3,2,1,0,15,14,13,12,11,10,9,8,7,6,5,4)),
                                            _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(3,2,1,0,15,14,13,12,11,10,9,8,7,6,5,4)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(1,0,15,14,13,12,11,10,9,8,7,6,5,4,3,2)),
                                            _mm_cmpeq_epi16(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(1,0,15,14,13,12,11,10,9,8,7,6,5,4,3,2))))))
                        ),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;
                    __m128i v_i = _mm_set1_epi16(first.data[i]);
                    bitmask = _mm_or_si128(bitmask, _mm_cmpeq_epi16(f_data, v_i));
                    has_mask = _mm_test_all_ones(bitmask);
                }
            }
        }
    }

    if (!has_mask && second.size > 2)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// SSE4.2 Int8_t specialization
template <>
inline ALWAYS_INLINE bool sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<int8_t>, NumericArraySlice<int8_t>, sliceEqualElements<int8_t,int8_t> >(
    const NumericArraySlice<int8_t> & first, const NumericArraySlice<int8_t> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    short has_mask = 1;
    const int full = -1, none = 0;
    const __m128i zeros = _mm_setzero_si128();
    if (second.size > 15)
    {
        for (; j < second.size - 15 && has_mask; j += 16)
        {
            has_mask = 0;
            const __m128i f_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(second.data+j));
            __m128i bitmask = has_second_null_map ?
                _mm_set_epi8(
                    (second_null_map[j + 15]) ? full : none, (second_null_map[j + 14]) ? full : none,
                    (second_null_map[j + 13]) ? full : none, (second_null_map[j + 12]) ? full : none,
                    (second_null_map[j + 11]) ? full : none, (second_null_map[j + 10]) ? full : none,
                    (second_null_map[j + 9]) ? full : none, (second_null_map[j + 8]) ? full : none,
                    (second_null_map[j + 7]) ? full : none, (second_null_map[j + 6]) ? full : none,
                    (second_null_map[j + 5]) ? full : none, (second_null_map[j + 4]) ? full : none,
                    (second_null_map[j + 3]) ? full : none, (second_null_map[j + 2]) ? full : none,
                    (second_null_map[j + 1]) ? full : none, (second_null_map[j]) ? full : none)
                : zeros;
            unsigned i = 0;
            for (; i < first.size - 15 && !has_mask; has_mask = _mm_test_all_ones(bitmask), i += 16)
            {
                const __m128i s_data = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(first.data+i));
                const __m128i first_nm_mask = has_first_null_map ?
                    _mm_lddqu_si128(reinterpret_cast<const __m128i *>(first_null_map+i))
                    : zeros;
                bitmask =
                    _mm_or_si128(
                        _mm_or_si128(
                            _mm_or_si128(
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            first_nm_mask,
                                            _mm_cmpeq_epi8(f_data, s_data)),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,15)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,15))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,15,14)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,15,14)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(12,11,10,9,8,7,6,5,4,3,2,1,0,15,14,13)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(12,11,10,9,8,7,6,5,4,3,2,1,0,15,14,13)))))
                                ),
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,15,14,13,12)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,15,14,13,12)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(10,9,8,7,6,5,4,3,2,1,0,15,14,13,12,11)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(10,9,8,7,6,5,4,3,2,1,0,15,14,13,12,11))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(9,8,7,6,5,4,3,2,1,0,15,14,13,12,11,10)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(9,8,7,6,5,4,3,2,1,0,15,14,13,12,11,10)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(8,7,6,5,4,3,2,1,0,15,14,13,12,11,10,9)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(8,7,6,5,4,3,2,1,0,15,14,13,12,11,10,9))))))),
                            _mm_or_si128(
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(7,6,5,4,3,2,1,0,15,14,13,12,11,10,9,8)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(7,6,5,4,3,2,1,0,15,14,13,12,11,10,9,8)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(6,5,4,3,2,1,0,15,14,13,12,11,10,9,8,7)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(6,5,4,3,2,1,0,15,14,13,12,11,10,9,8,7))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(5,4,3,2,1,0,15,14,13,12,11,10,9,8,7,6)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(5,4,3,2,1,0,15,14,13,12,11,10,9,8,7,6)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(4,3,2,1,0,15,14,13,12,11,10,9,8,7,6,5)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(4,3,2,1,0,15,14,13,12,11,10,9,8,7,6,5)))))),
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(3,2,1,0,15,14,13,12,11,10,9,8,7,6,5,4)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(3,2,1,0,15,14,13,12,11,10,9,8,7,6,5,4)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(2,1,0,15,14,13,12,11,10,9,8,7,6,5,4,3)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(2,1,0,15,14,13,12,11,10,9,8,7,6,5,4,3))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(1,0,15,14,13,12,11,10,9,8,7,6,5,4,3,2)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(1,0,15,14,13,12,11,10,9,8,7,6,5,4,3,2)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(0,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)),
                                            _mm_cmpeq_epi8(f_data, _mm_shuffle_epi8(s_data, _mm_set_epi8(0,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)))))))),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;
                    __m128i v_i = _mm_set1_epi8(first.data[i]);
                    bitmask = _mm_or_si128(bitmask, _mm_cmpeq_epi8(f_data, v_i));
                    has_mask = _mm_test_all_ones(bitmask);
                }
            }
        }
    }

    if (!has_mask)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

#endif


template < typename FirstSliceType,
           typename SecondSliceType,
           bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t),
           bool (*isEqualUnary)(const SecondSliceType &, size_t, size_t)>
bool sliceHasImplSubstr(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    std::vector<size_t> prefix_function;
    if (has_second_null_map)
    {
        prefix_function = buildKMPPrefixFunction(second,
                [null_map = second_null_map](const SecondSliceType & pattern, size_t i, size_t j)
                {
                    return !!null_map[i] == !!null_map[j] && (!!null_map[i] || isEqualUnary(pattern, i, j));
                });
    }
    else
    {
        prefix_function = buildKMPPrefixFunction(second,
                [](const SecondSliceType & pattern, size_t i, size_t j) { return isEqualUnary(pattern, i, j); });
    }

    size_t firstCur = 0;
    size_t secondCur = 0;
    while (firstCur < first.size && secondCur < second.size)
    {
        const bool is_first_null = has_first_null_map && first_null_map[firstCur];
        const bool is_second_null = has_second_null_map && second_null_map[secondCur];

        const bool cond_both_null_match = is_first_null && is_second_null;
        const bool cond_both_not_null = !is_first_null && !is_second_null;
        if (cond_both_null_match || (cond_both_not_null && isEqual(first, second, firstCur, secondCur)))
        {
            ++firstCur;
            ++secondCur;
        }
        else if (secondCur > 0)
        {
            secondCur = prefix_function[secondCur - 1];
        }
        else
        {
            ++firstCur;
        }
    }

    return secondCur == second.size;
}


template <
    ArraySearchType search_type,
    typename FirstSliceType,
    typename SecondSliceType,
    bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t),
    bool (*isEqualSecond)(const SecondSliceType &, size_t, size_t)>
bool sliceHasImpl(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if constexpr (search_type == ArraySearchType::Substr)
        return sliceHasImplSubstr<FirstSliceType, SecondSliceType, isEqual, isEqualSecond>(first, second, first_null_map, second_null_map);
    else
        return sliceHasImplAnyAll<search_type, FirstSliceType, SecondSliceType, isEqual>(first, second, first_null_map, second_null_map);
}


template <ArraySearchType search_type, typename T, typename U>
bool sliceHas(const NumericArraySlice<T> & first, const NumericArraySlice<U> & second)
{
    auto impl = sliceHasImpl<search_type, NumericArraySlice<T>, NumericArraySlice<U>, sliceEqualElements<T, U>, insliceEqualElements<U>>;
    return impl(first, second, nullptr, nullptr);
}

template <ArraySearchType search_type>
bool sliceHas(const GenericArraySlice & first, const GenericArraySlice & second)
{
    /// Generic arrays should have the same type in order to use column.compareAt(...)
    if (!first.elements->structureEquals(*second.elements))
        throw Exception("Function sliceHas expects same column types for slices.", ErrorCodes::LOGICAL_ERROR);

    auto impl = sliceHasImpl<search_type, GenericArraySlice, GenericArraySlice, sliceEqualElements, insliceEqualElements>;
    return impl(first, second, nullptr, nullptr);
}

template <ArraySearchType search_type, typename U>
bool sliceHas(const GenericArraySlice & /*first*/, const NumericArraySlice<U> & /*second*/)
{
    return false;
}

template <ArraySearchType search_type, typename T>
bool sliceHas(const NumericArraySlice<T> & /*first*/, const GenericArraySlice & /*second*/)
{
    return false;
}

template <ArraySearchType search_type, typename FirstArraySlice, typename SecondArraySlice>
bool sliceHas(const FirstArraySlice & first, NullableSlice<SecondArraySlice> & second)
{
    auto impl = sliceHasImpl<
        search_type,
        FirstArraySlice,
        SecondArraySlice,
        sliceEqualElements<FirstArraySlice, SecondArraySlice>,
        insliceEqualElements<SecondArraySlice>>;
    return impl(first, second, nullptr, second.null_map);
}

template <ArraySearchType search_type, typename FirstArraySlice, typename SecondArraySlice>
bool sliceHas(const NullableSlice<FirstArraySlice> & first, SecondArraySlice & second)
{
    auto impl = sliceHasImpl<
        search_type,
        FirstArraySlice,
        SecondArraySlice,
        sliceEqualElements<FirstArraySlice, SecondArraySlice>,
        insliceEqualElements<SecondArraySlice>>;
    return impl(first, second, first.null_map, nullptr);
}

template <ArraySearchType search_type, typename FirstArraySlice, typename SecondArraySlice>
bool sliceHas(const NullableSlice<FirstArraySlice> & first, NullableSlice<SecondArraySlice> & second)
{
    auto impl = sliceHasImpl<
        search_type,
        FirstArraySlice,
        SecondArraySlice,
        sliceEqualElements<FirstArraySlice, SecondArraySlice>,
        insliceEqualElements<SecondArraySlice>>;
    return impl(first, second, first.null_map, second.null_map);
}

template <ArraySearchType search_type, typename FirstSource, typename SecondSource>
void NO_INLINE arrayAllAny(FirstSource && first, SecondSource && second, ColumnUInt8 & result)
{
    auto size = result.size();
    auto & data = result.getData();
    for (auto row : collections::range(0, size))
    {
        data[row] = static_cast<UInt8>(sliceHas<search_type>(first.getWhole(), second.getWhole()));
        first.next();
        second.next();
    }
}

template <typename ArraySource, typename ValueSource, typename Sink>
void resizeDynamicSize(ArraySource && array_source, ValueSource && value_source, Sink && sink, const IColumn & size_column)
{
    const auto * size_nullable = typeid_cast<const ColumnNullable *>(&size_column);
    const NullMap * size_null_map = size_nullable ? &size_nullable->getNullMapData() : nullptr;
    const IColumn * size_nested_column = size_nullable ? &size_nullable->getNestedColumn() : &size_column;

    while (!sink.isEnd())
    {
        size_t row_num = array_source.rowNum();
        bool has_size = !size_null_map || (*size_null_map)[row_num];

        if (has_size)
        {
            auto size = size_nested_column->getInt(row_num);
            auto array_size = array_source.getElementSize();

            if (size >= 0)
            {
                size_t length = static_cast<size_t>(size);
                if (length > MAX_ARRAY_SIZE)
                    throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size: {}, maximum: {}",
                        length, MAX_ARRAY_SIZE);

                if (array_size <= length)
                {
                    writeSlice(array_source.getWhole(), sink);
                    for (size_t i = array_size; i < length; ++i)
                        writeSlice(value_source.getWhole(), sink);
                }
                else
                    writeSlice(array_source.getSliceFromLeft(0, length), sink);
            }
            else
            {
                size_t length = -static_cast<size_t>(size);
                if (length > MAX_ARRAY_SIZE)
                    throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size: {}, maximum: {}",
                        length, MAX_ARRAY_SIZE);

                if (array_size <= length)
                {
                    for (size_t i = array_size; i < length; ++i)
                        writeSlice(value_source.getWhole(), sink);
                    writeSlice(array_source.getWhole(), sink);
                }
                else
                    writeSlice(array_source.getSliceFromRight(length, length), sink);
            }
        }
        else
            writeSlice(array_source.getWhole(), sink);

        value_source.next();
        array_source.next();
        sink.next();
    }
}

template <typename ArraySource, typename ValueSource, typename Sink>
void resizeConstantSize(ArraySource && array_source, ValueSource && value_source, Sink && sink, const ssize_t size)
{
    while (!sink.isEnd())
    {
        auto array_size = array_source.getElementSize();

        if (size >= 0)
        {
            size_t length = static_cast<size_t>(size);
            if (length > MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size: {}, maximum: {}",
                    length, MAX_ARRAY_SIZE);

            if (array_size <= length)
            {
                writeSlice(array_source.getWhole(), sink);
                for (size_t i = array_size; i < length; ++i)
                    writeSlice(value_source.getWhole(), sink);
            }
            else
                writeSlice(array_source.getSliceFromLeft(0, length), sink);
        }
        else
        {
            size_t length = -static_cast<size_t>(size);
            if (length > MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size: {}, maximum: {}",
                    length, MAX_ARRAY_SIZE);

            if (array_size <= length)
            {
                for (size_t i = array_size; i < length; ++i)
                    writeSlice(value_source.getWhole(), sink);
                writeSlice(array_source.getWhole(), sink);
            }
            else
                writeSlice(array_source.getSliceFromRight(length, length), sink);
        }

        value_source.next();
        array_source.next();
        sink.next();
    }
}

}
