#pragma once

#include <base/types.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include "Sources.h"
#include "Sinks.h"
#include <Core/AccurateComparison.h>
#include <base/range.h>
#include "GatherUtils.h"
#include "sliceEqualElements.h"
#include "sliceHasImplAnyAll.h"


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

    auto check_and_get_size_to_reserve = [] (auto source, IArraySource * array_source)
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
        const auto & source = array_sources[i];
        is_const[i] = source->isConst();
        if (is_const[i])
            size_to_reserve += check_and_get_size_to_reserve(typeid_cast<ConstSource<Source> *>(source.get()), source.get());
        else
            size_to_reserve += check_and_get_size_to_reserve(typeid_cast<Source *>(source.get()), source.get());
    }

    sink.reserve(size_to_reserve);

    auto write_next = [& sink] (auto source)
    {
        writeSlice(source->getWhole(), sink);
        source->next();
    };

    while (!sink.isEnd())
    {
        for (auto i : collections::range(0, sources_num))
        {
            const auto & source = array_sources[i];
            if (is_const[i])
                write_next(static_cast<ConstSource<Source> *>(source.get()));
            else
                write_next(static_cast<Source *>(source.get()));
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


template <bool inverse, typename Source, typename Sink>
static void sliceDynamicOffsetBoundedImpl(Source && src, Sink && sink, const IColumn * offset_column, const IColumn * length_column)
{
    const bool is_offset_null = !offset_column || offset_column->onlyNull();
    const ColumnUInt8::Container * offset_null_map = nullptr;
    const IColumn * offset_nested_column = nullptr;

    if (!is_offset_null)
    {
        const auto * offset_nullable = typeid_cast<const ColumnNullable *>(offset_column);
        offset_null_map = offset_nullable ? &offset_nullable->getNullMapData() : nullptr;
        offset_nested_column = offset_nullable ? &offset_nullable->getNestedColumn() : offset_column;
    }

    const bool is_length_null = !length_column || length_column->onlyNull();
    const ColumnUInt8::Container * length_null_map = nullptr;
    const IColumn * length_nested_column = nullptr;

    if (!is_length_null)
    {
        const auto * length_nullable = typeid_cast<const ColumnNullable *>(length_column);
        length_null_map = length_nullable ? &length_nullable->getNullMapData() : nullptr;
        length_nested_column = length_nullable ? &length_nullable->getNestedColumn() : length_column;
    }

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
            {
                if constexpr (inverse)
                    slice = src.getSliceFromRight(UInt64(size) + UInt64(offset) - 1, size);
                else
                    slice = src.getSliceFromLeft(UInt64(offset) - 1, size);
            }
            else
            {
                if constexpr (inverse)
                    slice = src.getSliceFromLeft(-UInt64(offset), size);
                else
                    slice = src.getSliceFromRight(-UInt64(offset), size);
            }

            writeSlice(slice, sink);
        }

        sink.next();
        src.next();
    }
}


template <typename Source, typename Sink>
void NO_INLINE sliceDynamicOffsetBounded(Source && src, Sink && sink, const IColumn & offset_column, const IColumn & length_column)
{
    sliceDynamicOffsetBoundedImpl<false>(std::forward<Source>(src), std::forward<Sink>(sink), &offset_column, &length_column);
}

/// Similar to above, but with no offset.
template <typename Source, typename Sink>
void NO_INLINE sliceFromLeftDynamicLength(Source && src, Sink && sink, const IColumn & length_column)
{
    sliceDynamicOffsetBoundedImpl<false>(std::forward<Source>(src), std::forward<Sink>(sink), nullptr, &length_column);
}

template <typename Source, typename Sink>
void NO_INLINE sliceFromRightDynamicLength(Source && src, Sink && sink, const IColumn & length_column)
{
    sliceDynamicOffsetBoundedImpl<true>(std::forward<Source>(src), std::forward<Sink>(sink), nullptr, &length_column);
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

template <
    ArraySearchType search_type,
    typename FirstSliceType,
    typename SecondSliceType,
          bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t)>
bool sliceHasImplStartsEndsWith(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    if (first.size < second.size)
        return false;

    size_t first_index = (search_type == ArraySearchType::StartsWith) ? 0 : first.size - second.size;
    for (size_t second_index = 0; second_index < second.size; ++second_index, ++first_index)
    {
        const bool is_first_null = has_first_null_map && first_null_map[first_index];
        const bool is_second_null = has_second_null_map && second_null_map[second_index];
        if (is_first_null != is_second_null)
            return false;
        if (!is_first_null && !is_second_null && !isEqual(first, second, first_index, second_index))
            return false;
    }
    return true;
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

    size_t first_cur = 0;
    size_t second_cur = 0;
    while (first_cur < first.size && second_cur < second.size)
    {
        const bool is_first_null = has_first_null_map && first_null_map[first_cur];
        const bool is_second_null = has_second_null_map && second_null_map[second_cur];

        const bool cond_both_null_match = is_first_null && is_second_null;
        const bool cond_both_not_null = !is_first_null && !is_second_null;
        if (cond_both_null_match || (cond_both_not_null && isEqual(first, second, first_cur, second_cur)))
        {
            ++first_cur;
            ++second_cur;
        }
        else if (second_cur > 0)
        {
            second_cur = prefix_function[second_cur - 1];
        }
        else
        {
            ++first_cur;
        }
    }

    return second_cur == second.size;
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
    else if constexpr (search_type == ArraySearchType::StartsWith || search_type == ArraySearchType::EndsWith)
        return sliceHasImplStartsEndsWith<search_type, FirstSliceType, SecondSliceType, isEqual>(first, second, first_null_map, second_null_map);
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
