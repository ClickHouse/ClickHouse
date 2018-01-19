#pragma once

#include <Functions/GatherUtils/IArraySink.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Common/typeid_cast.h>

namespace DB::GatherUtils
{

template <typename T>
struct NumericArraySink : public ArraySinkImpl<NumericArraySink<T>>
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


struct GenericArraySink : public ArraySinkImpl<GenericArraySink>
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


template <typename ArraySink>
struct NullableArraySink : public ArraySink
{
    ColumnUInt8::Container & null_map;

    NullableArraySink(ColumnArray & arr, ColumnUInt8 & null_map, size_t column_size)
            : ArraySink(arr, column_size), null_map(null_map.getData())
    {
    }

    void accept(ArraySinkVisitor & visitor) override { visitor.visit(*this); }

    void reserve(size_t num_elements)
    {
        ArraySink::reserve(num_elements);
        null_map.reserve(num_elements);
    }
};


}
