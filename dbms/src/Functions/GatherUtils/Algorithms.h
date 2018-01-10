#pragma once

#include <Common/FieldVisitors.h>

#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/Sinks.h>

#include <ext/range.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace  DB::GatherUtils
{

/// Methods to copy Slice to Sink, overloaded for various combinations of types.

template <typename T>
void writeSlice(const NumericArraySlice<T> & slice, NumericArraySink<T> & sink)
{
    /// It's possible to write slice into the middle of numeric column. Used in numeric array concat.
    if (sink.elements.size() < sink.current_offset + slice.size)
        sink.elements.resize(sink.current_offset + slice.size);
    /// Can't use memcpySmallAllowReadWriteOverflow15 when need to write slice into the middle of numeric column.
    /// TODO: Implement more efficient memcpy without overflow.
    memcpy(&sink.elements[sink.current_offset], slice.data, slice.size * sizeof(T));
    sink.current_offset += slice.size;
}

template <typename T, typename U>
void writeSlice(const NumericArraySlice<T> & slice, NumericArraySink<U> & sink)
{
    /// It's possible to write slice into the middle of numeric column. Used in numeric array concat.
    if (sink.elements.size() < sink.current_offset + slice.size)
        sink.elements.resize(sink.current_offset + slice.size);
    for (size_t i = 0; i < slice.size; ++i)
    {
        sink.elements[sink.current_offset] = slice.data[i];
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
    if (typeid(slice.elements) == typeid(static_cast<const IColumn *>(&sink.elements)))
    {
        sink.elements.insertRangeFrom(*slice.elements, slice.begin, slice.size);
        sink.current_offset += slice.size;
    }
    else
        throw Exception("Function writeSlice expect same column types for GenericArraySlice and GenericArraySink.",
                        ErrorCodes::LOGICAL_ERROR);
}

template <typename T>
inline ALWAYS_INLINE void writeSlice(const GenericArraySlice & slice, NumericArraySink<T> & sink)
{
    /// It's possible to write slice into the middle of numeric column. Used in numeric array concat.
    if (sink.elements.size() < sink.current_offset + slice.size)
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
        Field field = static_cast<typename NearestFieldType<T>::Type>(slice.data[i]);
        sink.elements.insert(field);
    }
    sink.current_offset += slice.size;
}

template <typename ArraySlice, typename ArraySink>
inline ALWAYS_INLINE void writeSlice(const NullableArraySlice<ArraySlice> & slice, NullableArraySink<ArraySink> & sink)
{
    /// It's possible to write slice into the middle of numeric column. Used in numeric array concat.
    if (sink.null_map.size() < sink.current_offset + slice.size)
        sink.null_map.resize(sink.current_offset + slice.size);
    /// Can't use memcpySmallAllowReadWriteOverflow15 when need to write slice into the middle of numeric column.
    /// TODO: Implement more efficient memcpy without overflow.
    memcpy(&sink.null_map[sink.current_offset], slice.null_map, slice.size * sizeof(UInt8));
    writeSlice(static_cast<const ArraySlice &>(slice), static_cast<ArraySink &>(sink));
}

template <typename ArraySlice, typename ArraySink>
inline ALWAYS_INLINE void writeSlice(const ArraySlice & slice, NullableArraySink<ArraySink> & sink)
{
    /// It's possible to write slice into the middle of numeric column. Used in numeric array concat.
    if (sink.null_map.size() < sink.current_offset + slice.size)
        sink.null_map.resize(sink.current_offset + slice.size);
    /// Can't use memcpySmallAllowReadWriteOverflow15 when need to write slice into the middle of numeric column.
    /// TODO: Implement more efficient memcpy without overflow.
    memset(&sink.null_map[sink.current_offset], 0, slice.size * sizeof(UInt8));
    writeSlice(slice, static_cast<ArraySink &>(sink));
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
    const ColumnUInt8::Container * null_map = nullable ? &nullable->getNullMapColumn().getData() : nullptr;
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
                slice = src.getSliceFromRight(-offset);

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
    const ColumnUInt8::Container * offset_null_map = offset_nullable ? &offset_nullable->getNullMapColumn().getData() : nullptr;
    const IColumn * offset_nested_column = offset_nullable ? &offset_nullable->getNestedColumn() : &offset_column;

    const bool is_length_null = length_column.onlyNull();
    const auto * length_nullable = typeid_cast<const ColumnNullable *>(&length_column);
    const ColumnUInt8::Container * length_null_map = length_nullable ? &length_nullable->getNullMapColumn().getData() : nullptr;
    const IColumn * length_nested_column = length_nullable ? &length_nullable->getNestedColumn() : &length_column;

    while (!src.isEnd())
    {
        size_t row_num = src.rowNum();
        bool has_offset = !is_offset_null && !(offset_null_map && (*offset_null_map)[row_num]);
        bool has_length = !is_length_null && !(length_null_map && (*length_null_map)[row_num]);
        Int64 offset = has_offset ? offset_nested_column->getInt(row_num) : 1;
        Int64 size = has_length ? length_nested_column->getInt(row_num) : static_cast<Int64>(src.getElementSize());

        if (size < 0)
            size += offset > 0 ? static_cast<Int64>(src.getElementSize()) - (offset - 1) : -offset;

        if (offset != 0 && size > 0)
        {
            typename std::decay_t<Source>::Slice slice;

            if (offset > 0)
                slice = src.getSliceFromLeft(offset - 1, size);
            else
                slice = src.getSliceFromRight(-offset, size);

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

    const UInt8 * cond_pos = &condition[0];
    const UInt8 * cond_end = cond_pos + condition.size();

    while (cond_pos < cond_end)
    {
        if (*cond_pos)
            writeSlice(src_a.getWhole(), sink);
        else
            writeSlice(src_b.getWhole(), sink);

        ++cond_pos;
        src_a.next();
        src_b.next();
        sink.next();
    }
}


/// Methods to check if first array has elements from second array, overloaded for various combinations of types.

template <bool all, typename FirstSliceType, typename SecondSliceType,
          bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t)>
bool sliceHasImpl(const FirstSliceType & first, const SecondSliceType & second,
                  const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    for (size_t i = 0; i < second.size; ++i)
    {
        bool has = false;
        for (size_t j = 0; j < first.size && !has; ++j)
        {
            const bool is_first_null = has_first_null_map && first_null_map[j];
            const bool is_second_null = has_second_null_map && second_null_map[i];

            if (is_first_null && is_second_null)
                has = true;

            if (!is_first_null && !is_second_null && isEqual(first, second, j, i))
                has = true;
        }

        if (has && !all)
            return true;

        if (!has && all)
            return false;

    }

    return all;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename T, typename U>
bool sliceEqualElements(const NumericArraySlice<T> & first, const NumericArraySlice<U> & second, size_t first_ind, size_t second_ind)
{
    return first.data[first_ind] == second.data[second_ind];
}

#pragma GCC diagnostic pop

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

template <bool all, typename T, typename U>
bool sliceHas(const NumericArraySlice<T> & first, const NumericArraySlice<U> & second)
{
    auto impl = sliceHasImpl<all, NumericArraySlice<T>, NumericArraySlice<U>, sliceEqualElements<T, U>>;
    return impl(first, second, nullptr, nullptr);
}

template <bool all>
bool sliceHas(const GenericArraySlice & first, const GenericArraySlice & second)
{
    /// Generic arrays should have the same type in order to use column.compareAt(...)
    if (typeid(*first.elements) != typeid(*second.elements))
        return false;

    auto impl = sliceHasImpl<all, GenericArraySlice, GenericArraySlice, sliceEqualElements>;
    return impl(first, second, nullptr, nullptr);
}

template <bool all, typename U>
bool sliceHas(const GenericArraySlice & /*first*/, const NumericArraySlice<U> & /*second*/)
{
    return false;
}

template <bool all, typename T>
bool sliceHas(const NumericArraySlice<T> & /*first*/, const GenericArraySlice & /*second*/)
{
    return false;
}

template <bool all, typename FirstArraySlice, typename SecondArraySlice>
bool sliceHas(const FirstArraySlice & first, NullableArraySlice<SecondArraySlice> & second)
{
    auto impl = sliceHasImpl<all, FirstArraySlice, SecondArraySlice, sliceEqualElements<FirstArraySlice, SecondArraySlice>>;
    return impl(first, second, nullptr, second.null_map);
}

template <bool all, typename FirstArraySlice, typename SecondArraySlice>
bool sliceHas(const NullableArraySlice<FirstArraySlice> & first, SecondArraySlice & second)
{
    auto impl = sliceHasImpl<all, FirstArraySlice, SecondArraySlice, sliceEqualElements<FirstArraySlice, SecondArraySlice>>;
    return impl(first, second, first.null_map, nullptr);
}

template <bool all, typename FirstArraySlice, typename SecondArraySlice>
bool sliceHas(const NullableArraySlice<FirstArraySlice> & first, NullableArraySlice<SecondArraySlice> & second)
{
    auto impl = sliceHasImpl<all, FirstArraySlice, SecondArraySlice, sliceEqualElements<FirstArraySlice, SecondArraySlice>>;
    return impl(first, second, first.null_map, second.null_map);
}

template <bool all, typename FirstSource, typename SecondSource>
void NO_INLINE arrayAllAny(FirstSource && first, SecondSource && second, ColumnUInt8 & result)
{
    auto size = result.size();
    auto & data = result.getData();
    for (auto row : ext::range(0, size))
    {
        data[row] = static_cast<UInt8>(sliceHas<all>(first.getWhole(), second.getWhole()) ? 1 : 0);
        first.next();
        second.next();
    }
}

}
