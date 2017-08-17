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

#include <DataTypes/DataTypeTraits.h>
#include <iostream>
#include <typeindex>

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
    virtual const typename ColumnArray::Offsets_t & getOffsets() const = 0;
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

    const typename ColumnArray::Offsets_t & getOffsets() const
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
struct ConstSource : public Base
{
    using Slice = typename Base::Slice;
    using Column = ColumnConst;

    size_t total_rows;
    size_t row_num = 0;

    /// Base base;

    /// template <typename ColumnType>
    explicit ConstSource(const ColumnConst & col)
        : Base(static_cast<const typename Base::Column &>(col.getDataColumn())), total_rows(col.size())
    {
    }

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

//    Slice getWhole() const
//    {
//        return base.getWhole();
//    }
//
//    Slice getSliceFromLeft(size_t offset) const
//    {
//        return base.getSliceFromLeft(offset);
//    }
//
//    Slice getSliceFromLeft(size_t offset, size_t length) const
//    {
//        return base.getSliceFromLeft(offset, length);
//    }
//
//    Slice getSliceFromRight(size_t offset) const
//    {
//        return base.getSliceFromRight(offset);
//    }
//
//    Slice getSliceFromRight(size_t offset, size_t length) const
//    {
//        return base.getSliceFromRight(offset, length);
//    }
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

    size_t getSizeForReserve() const
    {
        return elements.size();
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


struct FixedStringSource
{
    using Slice = NumericArraySlice<UInt8>;
    using Column = ColumnFixedString;

    const UInt8 * pos;
    const UInt8 * end;
    size_t string_size;
    size_t row_num = 0;

    FixedStringSource(const ColumnFixedString & col)
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
        if (offset >= string_size)
            return {pos, 0};
        return {pos + string_size - offset, offset};
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        if (offset >= string_size)
            return {pos, 0};
        return {pos + string_size - offset, std::min(length, offset)};
    }
};


template <typename T>
struct NumericArraySink : public IArraySink
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

    void reserve(size_t num_elements)
    {
        elements.reserve(num_elements);
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
    ColumnString::Offset_t current_offset = 0;

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

    DynamicStringSource(const IColumn & col) : impl(static_cast<const typename Impl::Column &>(col)) {}

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

    const typename ColumnArray::Offsets_t & getOffsets() const
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

struct GenericArraySink : public IArraySink
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

    void reserve(size_t num_elements)
    {
        elements.reserve(num_elements);
    }
};


template <typename Slice>
struct NullableArraySlice : public Slice
{
    const UInt8 * null_map;

    NullableArraySlice() {}
    NullableArraySlice(const Slice & base) : Slice(base) {}
};


template <typename ArraySource>
struct NullableArraySource : public ArraySource
{
    using Slice = NullableArraySlice<typename ArraySource::Slice>;
    using ArraySource::prev_offset;
    using ArraySource::row_num;
    using ArraySource::offsets;

    const ColumnUInt8::Container_t & null_map;

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
        if (!slice.size)
            slice.null_map = &null_map[prev_offset];
        else
            slice.null_map = &null_map[prev_offset + offset];
        return slice;
    }

    Slice getSliceFromLeft(size_t offset, size_t length) const
    {
        Slice slice = ArraySource::getSliceFromLeft(offset, length);
        if (!slice.size)
            slice.null_map = &null_map[prev_offset];
        else
            slice.null_map = &null_map[prev_offset + offset];
        return slice;
    }

    Slice getSliceFromRight(size_t offset) const
    {
        Slice slice = ArraySource::getSliceFromRight(offset);
        if (!slice.size)
            slice.null_map = &null_map[prev_offset];
        else
            slice.null_map = &null_map[offsets[row_num] - offset];
        return slice;
    }

    Slice getSliceFromRight(size_t offset, size_t length) const
    {
        Slice slice = ArraySource::getSliceFromRight(offset, length);
        if (!slice.size)
            slice.null_map = &null_map[prev_offset];
        else
            slice.null_map = &null_map[offsets[row_num] - offset];
        return slice;
    }

    bool isNullable() const override
    {
        return true;
    }
};

template <typename ArraySink>
struct NullableArraySink : public ArraySink
{
    ColumnUInt8::Container_t & null_map;

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

template <typename ... Types>
struct ArraySourceCreator;

template <typename Type, typename ... Types>
struct ArraySourceCreator<Type, Types ...>
{
    static std::unique_ptr<IArraySource> create(const ColumnArray & col, const ColumnUInt8 * null_map, bool is_const, size_t total_rows)
    {
        std::cerr << "Numeric " << typeid(Type).name() << std::endl;
        if (typeid_cast<const ColumnVector<Type> *>(&col.getData()))
        {
            std::cerr << "Created" << typeid(NumericArraySource<Type>).name() << std::endl;
            if (null_map)
            {
                if (is_const)
                    return std::make_unique<ConstSource<NullableArraySource<NumericArraySource<Type>>>>(col, *null_map, total_rows);
                return std::make_unique<NullableArraySource<NumericArraySource<Type>>>(col, *null_map);
            }
            if (is_const)
                return std::make_unique<ConstSource<NumericArraySource<Type>>>(col, total_rows);
            return std::make_unique<NumericArraySource<Type>>(col);
        }

        return ArraySourceCreator<Types...>::create(col, null_map, is_const, total_rows);
    }
};

template <>
struct ArraySourceCreator<>
{
    static std::unique_ptr<IArraySource> create(const ColumnArray & col, const ColumnUInt8 * null_map, bool is_const, size_t total_rows)
    {
        std::cerr << "Generic " << typeid(GenericArraySource).name() << std::endl;
        if (null_map)
        {
            if (is_const)
                return std::make_unique<ConstSource<NullableArraySource<GenericArraySource>>>(col, *null_map, total_rows);
            return std::make_unique<NullableArraySource<GenericArraySource>>(col, *null_map);
        }
        if (is_const)
            return std::make_unique<ConstSource<GenericArraySource>>(col, total_rows);
        return std::make_unique<GenericArraySource>(col);
    }
};

inline std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows)
{
    using Creator = typename ApplyTypeListForClass<ArraySourceCreator, TypeListNumber>::Type;
    if (auto column_nullable = typeid_cast<const ColumnNullable *>(&col.getData()))
    {
        ColumnArray column(column_nullable->getNestedColumn(), col.getOffsetsColumn());
        return Creator::create(column, &column_nullable->getNullMapConcreteColumn(), is_const, total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}


template <typename ... Types>
struct ArraySinkCreator;

template <typename Type, typename ... Types>
struct ArraySinkCreator<Type, Types ...>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, ColumnUInt8 * null_map, size_t column_size)
    {
        if (typeid_cast<ColumnVector<Type> *>(&col.getData()))
        {
            if (null_map)
                return std::make_unique<NullableArraySink<NumericArraySink<Type>>>(col, *null_map, column_size);
            return std::make_unique<NumericArraySink<Type>>(col, column_size);
        }

        return ArraySinkCreator<Types ...>::create(col, null_map, column_size);
    }
};

template <>
struct ArraySinkCreator<>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, ColumnUInt8 * null_map, size_t column_size)
    {
        if (null_map)
            return std::make_unique<NullableArraySink<GenericArraySink>>(col, *null_map, column_size);
        return std::make_unique<GenericArraySink>(col, column_size);
    }
};

inline std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size)
{
    using Creator = ApplyTypeListForClass<ArraySinkCreator, TypeListNumber>::Type;
    if (auto column_nullable = typeid_cast<ColumnNullable *>(&col.getData()))
    {
        ColumnArray column(column_nullable->getNestedColumn(), col.getOffsetsColumn());
        return Creator::create(column, &column_nullable->getNullMapConcreteColumn(), column_size);
    }
    return Creator::create(col, nullptr, column_size);
}


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

    NumericSource(const Column & col)
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

    void reserve(size_t num_elements)
    {
    }
};


/// Methods to copy Slice to Sink, overloaded for various combinations of types.

template <typename T>
void writeSlice(const NumericArraySlice<T> & slice, NumericArraySink<T> & sink)
{
    if (sink.elements.size() < sink.current_offset + slice.size)
        sink.elements.resize(sink.current_offset + slice.size);
    memcpy(&sink.elements[sink.current_offset], slice.data, slice.size * sizeof(T));
    sink.current_offset += slice.size;
}

template <typename T, typename U>
void writeSlice(const NumericArraySlice<T> & slice, NumericArraySink<U> & sink)
{
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
    if (sink.elements.size() < sink.current_offset + slice.size)
        sink.elements.resize(sink.current_offset + slice.size);
    memcpySmallAllowReadWriteOverflow15(&sink.elements[sink.current_offset], slice.data, slice.size);
    sink.current_offset += slice.size;
}

inline ALWAYS_INLINE void writeSlice(const StringSource::Slice & slice, FixedStringSink & sink)
{
    memcpySmallAllowReadWriteOverflow15(&sink.elements[sink.current_offset], slice.data, slice.size);
}

inline ALWAYS_INLINE void writeSlice(const GenericArraySlice & slice, GenericArraySink & sink)
{
    if (std::type_index(typeid(slice.elements)) == std::type_index(typeid(&sink.elements)))
        sink.elements.insertRangeFrom(*slice.elements, slice.begin, slice.size);
    else
    {
        for (size_t i = 0; i < slice.size; ++i)
        {
            Field field;
            slice.elements->get(slice.begin + i, field);
            sink.elements.insert(field);
        }
    }
    sink.current_offset += slice.size;
}

template <typename T>
inline ALWAYS_INLINE void writeSlice(const GenericArraySlice & slice, NumericArraySink<T> & sink)
{
    if (sink.elements.size() < sink.current_offset + slice.size)
        sink.elements.resize(sink.current_offset + slice.size);
    for (size_t i = 0; i < slice.size; ++i)
    {
        Field field;
        slice.elements->get(slice.begin + i, field);
        sink.elements.push_back(field.get<T>());
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

/// Assuming same types of underlying columns for slice and sink if (ArraySlice, ArraySink) is (GenericArraySlice, GenericArraySink).
template <typename ArraySlice, typename ArraySink>
inline ALWAYS_INLINE void writeSlice(const NullableArraySlice<ArraySlice> & slice, NullableArraySink<ArraySink> & sink)
{
    if (sink.null_map.size() < sink.current_offset + slice.size)
        sink.null_map.resize(sink.current_offset + slice.size);
    memcpy(&sink.null_map[sink.current_offset], slice.null_map, slice.size * sizeof(UInt8));
    writeSlice(static_cast<const ArraySlice &>(slice), static_cast<ArraySink &>(sink));
}

/// Assuming same types of underlying columns for slice and sink if (ArraySlice, ArraySink) is (GenericArraySlice, GenericArraySink).
template <typename ArraySlice, typename ArraySink>
inline ALWAYS_INLINE void writeSlice(const ArraySlice & slice, NullableArraySink<ArraySink> & sink)
{
    if (sink.null_map.size() < sink.current_offset + slice.size)
        sink.null_map.resize(sink.current_offset + slice.size);
    memset(&sink.null_map[sink.current_offset], 0, slice.size * sizeof(UInt8));
    writeSlice(slice, static_cast<ArraySink &>(sink));
}

/// Algorithms

template <typename Source, typename Sink>
static void append(Source & source, Sink & sink)
{
    sink.row_num = 0;
    while (!source.isEnd())
    {
        sink.current_offset = sink.offsets[sink.row_num];
        writeSlice(source.getWhole(), sink);
        sink.next();
        source.next();
    }
}

template <template <typename ...> typename Base, typename ... Types>
struct ArraySourceSelector;

template <template <typename ...> typename Base, typename Type, typename ... Types>
struct ArraySourceSelector<Base, Type, Types ...>
{
    template <typename ... Args>
    static void select(IArraySource & source, Args & ... args)
    {
        if (auto array = typeid_cast<NumericArraySource<Type> *>(&source))
            Base<Types ...>::selectImpl(*array, args ...);
        else if(auto nullable_array = typeid_cast<NullableArraySource<NumericArraySource<Type>> *>(&source))
            Base<Types ...>::selectImpl(*nullable_array, args ...);
        else if (auto const_array = typeid_cast<ConstSource<NumericArraySource<Type>> *>(&source))
            Base<Types ...>::selectImpl(*const_array, args ...);
        else if(auto const_nullable_array = typeid_cast<ConstSource<NullableArraySource<NumericArraySource<Type>>> *>(&source))
            Base<Types ...>::selectImpl(*const_nullable_array, args ...);
        else
            Base<Types ...>::select(source, args ...);
    }
};

template <template <typename ...> typename Base>
struct ArraySourceSelector<Base>
{
    template <typename ... Args>
    static void select(IArraySource & source, Args & ... args)
    {
        if (auto array = typeid_cast<GenericArraySource *>(&source))
            Base<>::selectImpl(*array, args ...);
        else if(auto nullable_array = typeid_cast<NullableArraySource<GenericArraySource> *>(&source))
            Base<>::selectImpl(*nullable_array, args ...);
        else if (auto const_array = typeid_cast<ConstSource<GenericArraySource> *>(&source))
            Base<>::selectImpl(*const_array, args ...);
        else if(auto const_nullable_array = typeid_cast<ConstSource<NullableArraySource<GenericArraySource>> *>(&source))
            Base<>::selectImpl(*const_nullable_array, args ...);
        else
            throw Exception(std::string("Unknown ArraySource type: ") + typeid(source).name(), ErrorCodes::LOGICAL_ERROR);
    }
};


template <template <typename ...> typename Base, typename ... Types>
struct ArraySinkSelector;

template <template <typename ...> typename Base, typename Type, typename ... Types>
struct ArraySinkSelector<Base, Type, Types ...>
{
    template <typename ... Args>
    static void select(IArraySink & sink, Args & ... args)
    {
        if (auto nullable_numeric_sink = typeid_cast<NullableArraySink<NumericArraySink<Type>> *>(&sink))
            Base<>::selectImpl(*nullable_numeric_sink, args ...);
        else if (auto numeric_sink = typeid_cast<NumericArraySink<Type> *>(&sink))
            Base<>::selectImpl(*numeric_sink, args ...);
        else
            Base<Types ...>::select(sink, args ...);
    }
};

template <template <typename ...> typename Base>
struct ArraySinkSelector<Base>
{
    template <typename ... Args>
    static void select(IArraySink & sink, Args & ... args)
    {
        if (auto nullable_generic_sink = typeid_cast<NullableArraySink<GenericArraySink> *>(&sink))
            Base<>::selectImpl(*nullable_generic_sink, args ...);
        else if (auto generic_sink = typeid_cast<GenericArraySink *>(&sink))
            Base<>::selectImpl(*generic_sink, args ...);
        else
            throw Exception(std::string("Unknown ArraySink type: ") + typeid(sink).name(), ErrorCodes::LOGICAL_ERROR);
    }
};


template <typename ... Types>
struct ArrayAppend : public ArraySourceSelector<ArrayAppend, Types ...>
{
    template <typename Source, typename Sink>
    static void selectImpl(Source & source, Sink & sink)
    {
        append<Source, Sink>(source, sink);
    }
};

template <typename Sink>
static void append(IArraySource & source, Sink & sink)
{
    // using List = typename AppendToTypeList<Sink, TypeListNumber>::Type;
    using AppendImpl = typename ApplyTypeListForClass<ArrayAppend, TypeListNumber>::Type;
    AppendImpl::select(source, sink);
}


template <typename SourceA, typename SourceB, typename Sink>
void NO_INLINE concat(SourceA & src_a, SourceB & src_b, Sink & sink)
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

template <typename SourceA, typename SourceB, typename Sink>
void NO_INLINE concat(SourceA && src_a, SourceB && src_b, Sink && sink)
{
    concat(src_a, src_b, sink);
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

template <typename SourceType, typename SinkType>
struct ConcatGenericImpl
{
    static void concat(GenericArraySource * generic_source, SinkType & sink)
    {
        auto source = static_cast<SourceType *>(generic_source);
        writeSlice(source->getWhole(), sink);
        source->next();
    }
};

template <typename Sink>
static void NO_INLINE concatGeneric(const std::vector<std::unique_ptr<IArraySource>> & sources, Sink & sink)
{
    std::vector<GenericArraySource *> generic_sources;
    std::vector<bool> is_nullable;
    std::vector<bool> is_const;

    generic_sources.reserve(sources.size());
    is_nullable.assign(sources.size(), false);
    is_const.assign(sources.size(), false);

    for (auto i : ext::range(0, sources.size()))
    {
        const auto &source = sources[i];
        if (auto generic_source = typeid_cast<GenericArraySource *>(source.get()))
            generic_sources.push_back(static_cast<GenericArraySource *>(generic_source));
        else if (auto const_generic_source = typeid_cast<ConstSource<GenericArraySource> *>(source.get()))
        {
            generic_sources.push_back(static_cast<GenericArraySource *>(const_generic_source));
            is_const[i] = true;
        }
        else if (auto nullable_source = typeid_cast<NullableArraySource<GenericArraySource> *>(source.get()))
        {
            generic_sources.push_back(static_cast<GenericArraySource *>(nullable_source));
            is_nullable[i] = true;
        }
        else if (auto const_nullable_source = typeid_cast<ConstSource<NullableArraySource<GenericArraySource>> *>(source.get()))
        {
            generic_sources.push_back(static_cast<GenericArraySource *>(const_nullable_source));
            is_nullable[i] = is_const[i] = true;
        }
        else
            throw Exception(std::string("GenericArraySource expected for GenericArraySink, got: ") + typeid(source).name(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    while (!sink.isEnd())
    {
        for (auto i : ext::range(0, sources.size()))
        {
            auto source = generic_sources[i];
            if (is_const[i])
            {
                if (is_nullable[i])
                    ConcatGenericImpl<ConstSource<NullableArraySource<GenericArraySource>>, Sink>::concat(source, sink);
                else
                    ConcatGenericImpl<ConstSource<GenericArraySource>, Sink>::concat(source, sink);
            }
            else
            {
                if (is_nullable[i])
                    ConcatGenericImpl<NullableArraySource<GenericArraySource>, Sink>::concat(source, sink);
                else
                    ConcatGenericImpl<GenericArraySource, Sink>::concat(source, sink);
            }
        }
        sink.next();
    }
}

template <typename Sink>
void NO_INLINE concat(const std::vector<std::unique_ptr<IArraySource>> & sources, Sink & sink)
{
    size_t elements_to_reserve = 0;
    bool is_first = true;
    /// Prepare offsets column. Offsets should point to starts of result arrays.

    for (const auto & source : sources)
    {
        elements_to_reserve += source->getSizeForReserve();
        const auto & offsets = source->getOffsets();

        if (is_first)
        {
            sink.offsets.resize(source->getColumnSize());
            memset(&sink.offsets[0], 0, sink.offsets.size() * sizeof(offsets[0]));
            is_first = false;
        }
        std::cerr << "Offsets:";
        for (size_t i : ext::range(1, offsets.size()))
        {
            std::cerr << ' ' << offsets[i];
        }
        std::cerr << std::endl;

        if (source->isConst())
        {
            for (size_t i : ext::range(1, offsets.size()))
                sink.offsets[i] += offsets[0];
        }
        else
        {
            for (size_t i : ext::range(1, offsets.size()))
                sink.offsets[i] += offsets[i - 1] - (i > 1 ? offsets[i - 2] : 0);
        }
    }

    std::cerr << "Sink offsets:";
    for (size_t i : ext::range(1, sink.offsets.size()))
    {
        sink.offsets[i] += sink.offsets[i - 1];
        std::cerr << ' ' << sink.offsets[i];
    }
    std::cerr << std::endl;

    sink.reserve(elements_to_reserve);

    for (const auto & source : sources)
    {
        append<Sink>(*source, sink);
    }
}

template <typename ... Types>
struct ArrayConcat;

template <typename ... Types>
struct ArrayConcat : public ArraySinkSelector<ArrayConcat, Types ...>
{
    using Sources = std::vector<std::unique_ptr<IArraySource>>;

    template <typename Sink>
    static void selectImpl(Sink & sink, Sources & sources)
    {
        concat<Sink>(sources, sink);
    }

    static void selectImpl(GenericArraySink & sink, Sources & sources)
    {
        concatGeneric<GenericArraySink>(sources, sink);
    }

    static void selectImpl(NullableArraySink<GenericArraySink> & sink, Sources & sources)
    {
        concatGeneric<NullableArraySink<GenericArraySink>>(sources, sink);
    }
};


inline void concat(std::vector<std::unique_ptr<IArraySource>> & sources, IArraySink & sink)
{
    /// using List = typename AppendToTypeList<std::vector<std::unique_ptr<IArraySource>>, TypeListNumber>::Type;
    using ConcatImpl = typename ApplyTypeListForClass<ArrayConcat, TypeListNumber>::Type;
    using Sources = std::vector<std::unique_ptr<IArraySource>>;
    return ConcatImpl::select(sink, sources);
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
void NO_INLINE sliceFromLeftConstantOffsetBounded(Source && src, Sink && sink, size_t offset, size_t length)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromLeft(offset, length), sink);
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
void NO_INLINE sliceFromRightConstantOffsetBounded(Source && src, Sink && sink, size_t offset, size_t length)
{
    while (!src.isEnd())
    {
        writeSlice(src.getSliceFromRight(offset, length), sink);
        sink.next();
        src.next();
    }
}

template <typename Source, typename Sink>
void NO_INLINE sliceDynamicOffsetUnbounded(Source && src, Sink && sink, IColumn & offset_column)
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
void NO_INLINE sliceDynamicOffsetBounded(Source && src, Sink && sink, IColumn & offset_column, IColumn & length_column)
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
