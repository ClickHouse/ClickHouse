#pragma once

#include <type_traits>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>

#include <Functions/FunctionHelpers.h>

#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>


/** These methods are intended for implementation of functions, that
  *  copy ranges from one or more columns to another column.
  *
  * Example:
  * - concatenation of strings and arrays (concat);
  * - extracting slices and elements of strings and arrays (substring, arraySlice, arrayElement);
  * - creating arrays from several columns ([x, y]);
  * - conditional selecting from several string or array columns (if, multiIf);
  * - push and pop elements from array front or back (arrayPushBack, etc);
  * - splitting strings into arrays and joining arrays back;
  * - formatting strings (format).
  *
  * There are various Sources, Sinks and Slices.
  * Source - allows to iterate over a column and obtain Slices.
  * Slice - a reference to elements to copy.
  * Sink - allows to build result column by copying Slices into it.
  */

namespace DB
{

template <typename T>
struct NumericArraySlice
{
    const T * data;
    size_t size;
};


template <typename T>
struct NumericArraySource
{
    using Slice = NumericArraySlice<T>;
    using Column = ColumnArray;

    const typename ColumnVector<T>::Container_t & elements;
    const typename ColumnArray::Offsets_t & offsets;

    size_t row_num = 0;
    ColumnArray::Offset_t prev_offset = 0;

    NumericArraySource(const ColumnArray & arr)
        : elements(typeid_cast<const ColumnVector<T> &>(arr.getData()).getData()), offsets(arr.getOffsets())
    {
    }

    void next()
    {
        prev_offset = offsets[row_num];
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == offsets.size();
    }

    size_t rowNum() const
    {
        return row_num;
    }

    Slice getWhole() const
    {
        return {&elements[prev_offset], offsets[row_num] - prev_offset};
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + offset], elem_size - offset};
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + offset], std::min(length, elem_size - offset)};
    }

    Slice getSliceFromRight(size_t offset) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[offsets[row_num] - offset], offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[offsets[row_num] - offset], std::min(length, offset)};
    }
};


template <typename Base>
struct ConstSource
{
    using Slice = typename Base::Slice;
    using Column = ColumnConst;

    size_t total_rows;
    size_t row_num = 0;

    Base base;

    ConstSource(const ColumnConst & col)
        : total_rows(col.size()), base(static_cast<const typename Base::Column &>(col.getDataColumn()))
    {
    }

    void next()
    {
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == total_rows;
    }

    size_t rowNum() const
    {
        return row_num;
    }

    Slice getWhole() const
    {
        return base.getWhole();
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        return base.getSliceFromLeft(offset);
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        return base.getSliceFromLeft(offset, length);
    }

    Slice getSliceFromRight(size_t offset) const
    {
        return base.getSliceFromRight(offset);
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        return base.getSliceFromRight(offset, length);
    }
};


template <typename T>
struct NumericArraySink
{
    typename ColumnVector<T>::Container_t & elements;
    typename ColumnArray::Offsets_t & offsets;

    size_t row_num = 0;
    ColumnArray::Offset_t current_offset = 0;

    NumericArraySink(ColumnArray & arr, size_t column_size)
        : elements(typeid_cast<ColumnVector<T> &>(arr.getData()).getData()), offsets(arr.getOffsets())
    {
        offsets.resize(column_size);
    }

    void next()
    {
        offsets[row_num] = current_offset;
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == offsets.size();
    }

    size_t rowNum() const
    {
        return row_num;
    }
};


struct StringSource
{
    using Slice = NumericArraySlice<UInt8>;
    using Column = ColumnString;

    const typename ColumnString::Chars_t & elements;
    const typename ColumnString::Offsets_t & offsets;

    size_t row_num = 0;
    ColumnString::Offset_t prev_offset = 0;

    StringSource(const ColumnString & col)
        : elements(col.getChars()), offsets(col.getOffsets())
    {
    }

    void next()
    {
        prev_offset = offsets[row_num];
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == offsets.size();
    }

    size_t rowNum() const
    {
        return row_num;
    }

    Slice getWhole() const
    {
        return {&elements[prev_offset], offsets[row_num] - prev_offset - 1};
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        size_t elem_size = offsets[row_num] - prev_offset - 1;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + offset], elem_size - offset};
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset - 1;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + offset], std::min(length, elem_size - offset)};
    }

    Slice getSliceFromRight(size_t offset) const
    {
        size_t elem_size = offsets[row_num] - prev_offset - 1;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + elem_size - offset], offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset - 1;
        if (offset >= elem_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + elem_size - offset], std::min(length, offset)};
    }
};


struct StringSink
{
    typename ColumnString::Chars_t & elements;
    typename ColumnString::Offsets_t & offsets;

    size_t row_num = 0;
    ColumnString::Offset_t current_offset = 0;

    StringSink(ColumnString & col, size_t column_size)
        : elements(col.getChars()), offsets(col.getOffsets())
    {
        offsets.resize(column_size);
    }

    void next()
    {
        elements.push_back(0);
        ++current_offset;
        offsets[row_num] = current_offset;
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == offsets.size();
    }

    size_t rowNum() const
    {
        return row_num;
    }
};


struct FixedStringSource
{
    using Slice = NumericArraySlice<UInt8>;
    using Column = ColumnFixedString;

    const typename ColumnString::Chars_t & elements;
    size_t string_size;

    size_t row_num = 0;
    size_t total_rows;

    ColumnString::Offset_t prev_offset = 0;

    FixedStringSource(const ColumnFixedString & col)
        : elements(col.getChars()), string_size(col.getN()), total_rows(col.size())
    {
    }

    void next()
    {
        prev_offset += string_size;
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == total_rows;
    }

    size_t rowNum() const
    {
        return row_num;
    }

    Slice getWhole() const
    {
        return {&elements[prev_offset], string_size};
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        if (offset >= string_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + offset], string_size - offset};
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        if (offset >= string_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + offset], std::min(length, string_size - offset)};
    }

    Slice getSliceFromRight(size_t offset) const
    {
        if (offset >= string_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + string_size - offset], offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        if (offset >= string_size)
            return {&elements[prev_offset], 0};
        return {&elements[prev_offset + string_size - offset], std::min(length, offset)};
    }
};


struct FixedStringSink
{
    typename ColumnString::Chars_t & elements;
    size_t string_size;

    size_t row_num = 0;
    size_t total_rows;
    ColumnString::Offset_t current_offset = 0;

    FixedStringSink(ColumnFixedString & col, size_t column_size)
        : elements(col.getChars()), total_rows(column_size)
    {
        elements.resize(column_size * string_size);
    }

    void next()
    {
        current_offset += string_size;
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == total_rows;
    }

    size_t rowNum() const
    {
        return row_num;
    }
};


struct IStringSource
{
    using Slice = NumericArraySlice<UInt8>;

    virtual void next() = 0;
    virtual bool isEnd() const = 0;
    virtual Slice getWhole() const = 0;
    virtual ~IStringSource() {}
};

template <typename Impl>
struct DynamicStringSource : IStringSource
{
    Impl impl;

    DynamicStringSource(const IColumn & col) : impl(static_cast<const typename Impl::Column &>(col)) {}

    void next() override { impl.next(); };
    bool isEnd() const override { return impl.isEnd(); };
    Slice getWhole() const override { return impl.getWhole(); };
};

inline std::unique_ptr<IStringSource> createDynamicStringSource(const IColumn & col)
{
    if (checkColumn<ColumnString>(&col))
        return std::make_unique<DynamicStringSource<StringSource>>(col);
    if (checkColumn<ColumnFixedString>(&col))
        return std::make_unique<DynamicStringSource<FixedStringSource>>(col);
    if (checkColumnConst<ColumnString>(&col))
        return std::make_unique<DynamicStringSource<ConstSource<StringSource>>>(col);
    if (checkColumnConst<ColumnFixedString>(&col))
        return std::make_unique<DynamicStringSource<ConstSource<FixedStringSource>>>(col);
    throw Exception("Unexpected type of string column: " + col.getName(), ErrorCodes::ILLEGAL_COLUMN);
}

using StringSources = std::vector<std::unique_ptr<IStringSource>>;



struct GenericArraySlice
{
    const IColumn * elements;
    size_t begin;
    size_t size;
};


struct GenericArraySource
{
    using Slice = GenericArraySlice;

    const IColumn & elements;
    const typename ColumnArray::Offsets_t & offsets;

    size_t row_num = 0;
    ColumnArray::Offset_t prev_offset = 0;

    GenericArraySource(const ColumnArray & arr)
        : elements(arr.getData()), offsets(arr.getOffsets())
    {
    }

    void next()
    {
        prev_offset = offsets[row_num];
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == offsets.size();
    }

    size_t rowNum() const
    {
        return row_num;
    }

    Slice getWhole() const
    {
        return {&elements, prev_offset, offsets[row_num] - prev_offset};
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements, prev_offset, 0};
        return {&elements, prev_offset + offset, elem_size - offset};
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements, prev_offset, 0};
        return {&elements, prev_offset + offset, std::min(length, elem_size - offset)};
    }

    Slice getSliceFromRight(size_t offset) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements, prev_offset, 0};
        return {&elements, offsets[row_num] - offset, offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset >= elem_size)
            return {&elements, prev_offset, 0};
        return {&elements, offsets[row_num] - offset, std::min(length, offset)};
    }
};

struct GenericArraySink
{
    IColumn & elements;
    ColumnArray::Offsets_t & offsets;

    size_t row_num = 0;
    ColumnArray::Offset_t current_offset = 0;

    GenericArraySink(ColumnArray & arr, size_t column_size)
        : elements(arr.getData()), offsets(arr.getOffsets())
    {
        offsets.resize(column_size);
    }

    void next()
    {
        offsets[row_num] = current_offset;
        ++row_num;
    }

    bool isEnd() const
    {
        return row_num == offsets.size();
    }

    size_t rowNum() const
    {
        return row_num;
    }
};


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
    sink.elements.resize(sink.current_offset + slice.size);
    for (size_t i = 0; i < slice.size; ++i)
    {
        sink.elements[sink.current_offset] = slice.data[i];
        ++sink.current_offset;
    }
}

inline void writeSlice(const StringSource::Slice & slice, StringSink & sink)
{
    sink.elements.resize(sink.current_offset + slice.size);
    memcpySmallAllowReadWriteOverflow15(&sink.elements[sink.current_offset], slice.data, slice.size);
    sink.current_offset += slice.size;
}

/// Assuming same types of underlying columns for slice and sink.
inline void writeSlice(const GenericArraySlice & slice, GenericArraySink & sink)
{
    sink.elements.insertRangeFrom(*slice.elements, slice.begin, slice.size);
    sink.current_offset += slice.size;
}


/// Algorithms

template <typename SourceA, typename SourceB, typename Sink>
void concat(SourceA && src_a, SourceB && src_b, Sink && sink)
{
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
void concat(StringSources & sources, Sink && sink)
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
void sliceFromLeftConstantOffsetUnbounded(Source && src, Sink && sink, size_t offset)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromLeft(offset), sink);
        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void sliceFromLeftConstantOffsetBounded(Source && src, Sink && sink, size_t offset, size_t length)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromLeft(offset, length), sink);
        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void sliceFromRightConstantOffsetUnbounded(Source && src, Sink && sink, size_t offset)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromRight(offset), sink);
        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void sliceFromRightConstantOffsetBounded(Source && src, Sink && sink, size_t offset, size_t length)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromRight(offset, length), sink);
        sink.next();
        src.next();
    }
}


template <typename Source, typename Sink>
void sliceDynamicOffsetUnbounded(Source && src, Sink && sink, IColumn & offset_column)
{
    while (!src.isEnd())
    {
        Int64 offset = offset_column.getInt(src.rowNum());

        if (offset != 0)
        {
            typename std::decay<Source>::type::Slice slice;

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
void sliceDynamicOffsetBounded(Source && src, Sink && sink, IColumn & offset_column, IColumn & length_column)
{
    while (!src.isEnd())
    {
        size_t row_num = src.rowNum();
        Int64 offset = offset_column.getInt(row_num);
        UInt64 size = length_column.getInt(row_num);

        if (offset != 0 && size < 0x8000000000000000ULL)
        {
            typename std::decay<Source>::type::Slice slice;

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

}
