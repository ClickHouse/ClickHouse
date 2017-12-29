#pragma once

#include <type_traits>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Functions/FunctionHelpers.h>
#include <DataTypes/NumberTraits.h>

#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>
#include <ext/range.h>
#include <Core/TypeListNumber.h>
#include <Common/FieldVisitors.h>

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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename T>
struct NumericArraySlice
{
    const T * data;
    size_t size;
};


struct IArraySource
{
    virtual ~IArraySource() {}

    virtual size_t getSizeForReserve() const = 0;
    virtual const typename ColumnArray::Offsets & getOffsets() const = 0;
    virtual size_t getColumnSize() const = 0;
    virtual bool isConst() const { return false; }
    virtual bool isNullable() const { return false; }
};
struct IArraySink
{
    virtual ~IArraySink() {}
};


template <typename T>
struct NumericArraySource : public IArraySource
{
    using Slice = NumericArraySlice<T>;
    using Column = ColumnArray;

    const typename ColumnVector<T>::Container & elements;
    const typename ColumnArray::Offsets & offsets;

    size_t row_num = 0;
    ColumnArray::Offset prev_offset = 0;

    explicit NumericArraySource(const ColumnArray & arr)
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

    const typename ColumnArray::Offsets & getOffsets() const override
    {
        return offsets;
    }

    /// Get size for corresponding call or Sink::reserve to reserve memory for elements.
    size_t getSizeForReserve() const override
    {
        return elements.size();
    }

    size_t getColumnSize() const override
    {
        return offsets.size();
    }

    size_t getElementSize() const
    {
        return offsets[row_num] - prev_offset;
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
        if (offset > elem_size)
            return {&elements[prev_offset], elem_size};
        return {&elements[offsets[row_num] - offset], offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset > elem_size)
            return {&elements[prev_offset], length + elem_size > offset ? std::min(elem_size, length + elem_size - offset) : 0};
        return {&elements[offsets[row_num] - offset], std::min(length, offset)};
    }
};


template <typename Base>
struct ConstSource : public Base
{
    using Slice = typename Base::Slice;
    using Column = ColumnConst;

    size_t total_rows;
    size_t row_num = 0;

    explicit ConstSource(const ColumnConst & col)
        : Base(static_cast<const typename Base::Column &>(col.getDataColumn())), total_rows(col.size())
    {
    }

    /// Constructors for NullableArraySource.

    template <typename ColumnType>
    ConstSource(const ColumnType & col, size_t total_rows) : Base(col), total_rows(total_rows)
    {
    }

    template <typename ColumnType>
    ConstSource(const ColumnType & col, const ColumnUInt8 & null_map, size_t total_rows) : Base(col, null_map), total_rows(total_rows)
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

    size_t getSizeForReserve() const
    {
        return total_rows * Base::getSizeForReserve();
    }

    size_t getColumnSize() const // overrides for IArraySource
    {
        return total_rows;
    }

    bool isConst() const // overrides for IArraySource
    {
        return true;
    }
};


struct StringSource
{
    using Slice = NumericArraySlice<UInt8>;
    using Column = ColumnString;

    const typename ColumnString::Chars_t & elements;
    const typename ColumnString::Offsets & offsets;

    size_t row_num = 0;
    ColumnString::Offset prev_offset = 0;

    explicit StringSource(const ColumnString & col)
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

    size_t getSizeForReserve() const
    {
        return elements.size();
    }

    size_t getElementSize() const
    {
        return offsets[row_num] - prev_offset;
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
        if (offset > elem_size)
            return {&elements[prev_offset], elem_size};
        return {&elements[prev_offset + elem_size - offset], offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset - 1;
        if (offset > elem_size)
            return {&elements[prev_offset], length + elem_size > offset ? std::min(elem_size, length + elem_size - offset) : 0};
        return {&elements[prev_offset + elem_size - offset], std::min(length, offset)};
    }
};


struct FixedStringSource
{
    using Slice = NumericArraySlice<UInt8>;
    using Column = ColumnFixedString;

    const UInt8 * pos;
    const UInt8 * end;
    size_t string_size;
    size_t row_num = 0;

    explicit FixedStringSource(const ColumnFixedString & col)
        : string_size(col.getN())
    {
        const auto & chars = col.getChars();
        pos = chars.data();
        end = pos + chars.size();
    }

    void next()
    {
        pos += string_size;
        ++row_num;
    }

    bool isEnd() const
    {
        return pos == end;
    }

    size_t rowNum() const
    {
        return row_num;
    }

    size_t getSizeForReserve() const
    {
        return end - pos;
    }

    size_t getElementSize() const
    {
        return string_size;
    }

    Slice getWhole() const
    {
        return {pos, string_size};
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        if (offset >= string_size)
            return {pos, 0};
        return {pos + offset, string_size - offset};
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        if (offset >= string_size)
            return {pos, 0};
        return {pos + offset, std::min(length, string_size - offset)};
    }

    Slice getSliceFromRight(size_t offset) const
    {
        if (offset > string_size)
            return {pos, string_size};
        return {pos + string_size - offset, offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        if (offset > string_size)
            return {pos, length + string_size > offset ? std::min(string_size, length + string_size - offset) : 0};
        return {pos + string_size - offset, std::min(length, offset)};
    }
};


template <typename T>
struct NumericArraySink : public IArraySink
{
    typename ColumnVector<T>::Container & elements;
    typename ColumnArray::Offsets & offsets;

    size_t row_num = 0;
    ColumnArray::Offset current_offset = 0;

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

    void reserve(size_t num_elements)
    {
        elements.reserve(num_elements);
    }
};


struct StringSink
{
    typename ColumnString::Chars_t & elements;
    typename ColumnString::Offsets & offsets;

    size_t row_num = 0;
    ColumnString::Offset current_offset = 0;

    StringSink(ColumnString & col, size_t column_size)
        : elements(col.getChars()), offsets(col.getOffsets())
    {
        offsets.resize(column_size);
    }

    void ALWAYS_INLINE next()
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

    void reserve(size_t num_elements)
    {
        elements.reserve(num_elements);
    }
};


struct FixedStringSink
{
    typename ColumnString::Chars_t & elements;
    size_t string_size;

    size_t row_num = 0;
    size_t total_rows;
    ColumnString::Offset current_offset = 0;

    FixedStringSink(ColumnFixedString & col, size_t column_size)
        : elements(col.getChars()), string_size(col.getN()), total_rows(column_size)
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

    void reserve(size_t num_elements)
    {
        elements.reserve(num_elements);
    }
};


struct IStringSource
{
    using Slice = NumericArraySlice<UInt8>;

    virtual void next() = 0;
    virtual bool isEnd() const = 0;
    virtual size_t getSizeForReserve() const = 0;
    virtual Slice getWhole() const = 0;
    virtual ~IStringSource() {}
};

template <typename Impl>
struct DynamicStringSource final : IStringSource
{
    Impl impl;

    explicit DynamicStringSource(const IColumn & col) : impl(static_cast<const typename Impl::Column &>(col)) {}

    void next() override { impl.next(); }
    bool isEnd() const override { return impl.isEnd(); }
    size_t getSizeForReserve() const override { return impl.getSizeForReserve(); }
    Slice getWhole() const override { return impl.getWhole(); }
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


struct GenericArraySource : public IArraySource
{
    using Slice = GenericArraySlice;
    using Column = ColumnArray;

    const IColumn & elements;
    const typename ColumnArray::Offsets & offsets;

    size_t row_num = 0;
    ColumnArray::Offset prev_offset = 0;

    explicit GenericArraySource(const ColumnArray & arr)
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

    const typename ColumnArray::Offsets & getOffsets() const override
    {
        return offsets;
    }

    size_t getSizeForReserve() const override
    {
        return elements.size();
    }

    size_t getColumnSize() const override
    {
        return elements.size();
    }

    size_t getElementSize() const
    {
        return offsets[row_num] - prev_offset;
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
        if (offset > elem_size)
            return {&elements, prev_offset, elem_size};
        return {&elements, offsets[row_num] - offset, offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        size_t elem_size = offsets[row_num] - prev_offset;
        if (offset > elem_size)
            return {&elements, prev_offset, length + elem_size > offset ? std::min(elem_size, length + elem_size - offset) : 0};
        return {&elements, offsets[row_num] - offset, std::min(length, offset)};
    }
};

struct GenericArraySink : public IArraySink
{
    IColumn & elements;
    ColumnArray::Offsets & offsets;

    size_t row_num = 0;
    ColumnArray::Offset current_offset = 0;

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

    void reserve(size_t num_elements)
    {
        elements.reserve(num_elements);
    }
};


template <typename Slice>
struct NullableArraySlice : public Slice
{
    const UInt8 * null_map = nullptr;

    NullableArraySlice() = default;
    NullableArraySlice(const Slice & base) : Slice(base) {}
};


template <typename ArraySource>
struct NullableArraySource : public ArraySource
{
    using Slice = NullableArraySlice<typename ArraySource::Slice>;
    using ArraySource::prev_offset;
    using ArraySource::row_num;
    using ArraySource::offsets;

    const ColumnUInt8::Container & null_map;

    NullableArraySource(const ColumnArray & arr, const ColumnUInt8 & null_map)
            : ArraySource(arr), null_map(null_map.getData())
    {
    }

    Slice getWhole() const
    {
        Slice slice = ArraySource::getWhole();
        slice.null_map = &null_map[prev_offset];
        return slice;
    }

    Slice getSliceFromLeft(size_t offset) const
    {
        Slice slice = ArraySource::getSliceFromLeft(offset);
        if (offsets[row_num] > prev_offset + offset)
            slice.null_map = &null_map[prev_offset + offset];
        else
            slice.null_map = &null_map[prev_offset];
        return slice;
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        Slice slice = ArraySource::getSliceFromLeft(offset, length);
        if (offsets[row_num] > prev_offset + offset)
            slice.null_map = &null_map[prev_offset + offset];
        else
            slice.null_map = &null_map[prev_offset];
        return slice;
    }

    Slice getSliceFromRight(size_t offset) const
    {
        Slice slice = ArraySource::getSliceFromRight(offset);
        if (offsets[row_num] > prev_offset + offset)
            slice.null_map = &null_map[offsets[row_num] - offset];
        else
            slice.null_map = &null_map[prev_offset];
        return slice;
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        Slice slice = ArraySource::getSliceFromRight(offset, length);
        if (offsets[row_num] > prev_offset + offset)
            slice.null_map = &null_map[offsets[row_num] - offset];
        else
            slice.null_map = &null_map[prev_offset];
        return slice;
    }

    bool isNullable() const
    {
        return true;
    }
};

template <typename ArraySink>
struct NullableArraySink : public ArraySink
{
    ColumnUInt8::Container & null_map;

    NullableArraySink(ColumnArray & arr, ColumnUInt8 & null_map, size_t column_size)
        : ArraySink(arr, column_size), null_map(null_map.getData())
    {
    }

    void reserve(size_t num_elements)
    {
        ArraySink::reserve(num_elements);
        null_map.reserve(num_elements);
    }
};


std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows);
std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size);


template <typename T>
using NumericSlice = const T *;

template <typename T>
struct NumericSource
{
    using Slice = NumericSlice<T>;
    using Column = ColumnVector<T>;

    const T * begin;
    const T * pos;
    const T * end;

    explicit NumericSource(const Column & col)
    {
        const auto & container = col.getData();
        begin = container.data();
        pos = begin;
        end = begin + container.size();
    }

    void next()
    {
        ++pos;
    }

    bool isEnd() const
    {
        return pos == end;
    }

    size_t rowNum() const
    {
        return pos - begin;
    }

    size_t getSizeForReserve() const
    {
        return 0;   /// Simple numeric columns are resized before fill, no need to reserve.
    }

    Slice getWhole() const
    {
        return pos;
    }
};

template <typename T>
struct NumericSink
{
    T * begin;
    T * pos;
    T * end;

    NumericSink(ColumnVector<T> & col, size_t column_size)
    {
        auto & container = col.getData();
        container.resize(column_size);
        begin = container.data();
        pos = begin;
        end = begin + container.size();
    }

    void next()
    {
        ++pos;
    }

    bool isEnd() const
    {
        return pos == end;
    }

    size_t rowNum() const
    {
        return pos - begin;
    }

    void reserve(size_t /*num_elements*/)
    {
    }
};


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

/// Algorithms

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

void concat(std::vector<std::unique_ptr<IArraySource>> & sources, IArraySink & sink);


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


void sliceFromLeftConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset);

void sliceFromLeftConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length);

void sliceFromRightConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset);

void sliceFromRightConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length);

void sliceDynamicOffsetUnbounded(IArraySource & src, IArraySink & sink, const IColumn & offset_column);

void sliceDynamicOffsetBounded(IArraySource & src, IArraySink & sink, const IColumn & offset_column, const IColumn & length_column);

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

}
