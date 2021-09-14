#pragma once

#include "IArraySink.h"

#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace DB::GatherUtils
{
#pragma GCC visibility push(hidden)

template <typename T>
struct NumericArraySource;

struct GenericArraySource;

template <typename ArraySource>
struct NullableArraySource;

template <typename T>
struct NumericValueSource;

struct GenericValueSource;

template <typename ArraySource>
struct NullableValueSource;

template <typename T>
struct NumericArraySink : public ArraySinkImpl<NumericArraySink<T>>
{
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using CompatibleArraySource = NumericArraySource<T>;
    using CompatibleValueSource = NumericValueSource<T>;

    typename ColVecType::Container & elements;
    typename ColumnArray::Offsets & offsets;

    size_t row_num = 0;
    ColumnArray::Offset current_offset = 0;

    NumericArraySink(IColumn & elements_, ColumnArray::Offsets & offsets_, size_t column_size)
            : elements(assert_cast<ColVecType&>(elements_).getData()), offsets(offsets_)
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
    typename ColumnString::Chars & elements;
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
    typename ColumnString::Chars & elements;
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
    using CompatibleArraySource = GenericArraySource;
    using CompatibleValueSource = GenericValueSource;

    IColumn & elements;
    ColumnArray::Offsets & offsets;

    size_t row_num = 0;
    ColumnArray::Offset current_offset = 0;

    GenericArraySink(IColumn & elements_, ColumnArray::Offsets & offsets_, size_t column_size)
            : elements(elements_), offsets(offsets_)
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
    using CompatibleArraySource = NullableArraySource<typename ArraySink::CompatibleArraySource>;
    using CompatibleValueSource = NullableValueSource<typename ArraySink::CompatibleValueSource>;

    NullMap & null_map;

    NullableArraySink(IColumn & elements_, ColumnArray::Offsets & offsets_, size_t column_size)
        : ArraySink(assert_cast<ColumnNullable &>(elements_).getNestedColumn(), offsets_, column_size)
        , null_map(assert_cast<ColumnNullable &>(elements_).getNullMapData())
    {
    }

    void accept(ArraySinkVisitor & visitor) override { visitor.visit(*this); }

    void reserve(size_t num_elements)
    {
        ArraySink::reserve(num_elements);
        null_map.reserve(num_elements);
    }
};

#pragma GCC visibility pop
}
