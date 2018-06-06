#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Common/typeid_cast.h>

#include <Functions/GatherUtils/IArraySource.h>
#include <Functions/GatherUtils/IValueSource.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/FunctionHelpers.h>

namespace DB::GatherUtils
{

template <typename T>
struct NumericArraySource : public ArraySourceImpl<NumericArraySource<T>>
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

    template <typename ColumnType>
    ConstSource(const ColumnType & col, size_t total_rows) : Base(col), total_rows(total_rows)
    {
    }

    template <typename ColumnType>
    ConstSource(const ColumnType & col, const NullMap & null_map, size_t total_rows) : Base(col, null_map), total_rows(total_rows)
    {
    }

    virtual ~ConstSource() = default;

    virtual void accept(ArraySourceVisitor & visitor) // override
    {
        if constexpr (std::is_base_of<IArraySource, Base>::value)
            visitor.visit(*this);
        else
            throw Exception(
                    "accept(ArraySourceVisitor &) is not implemented for " + demangle(typeid(ConstSource<Base>).name())
                    + " because " + demangle(typeid(Base).name()) + " is not derived from IArraySource ");
    }

    virtual void accept(ValueSourceVisitor & visitor) // override
    {
        if constexpr (std::is_base_of<IValueSource, Base>::value)
            visitor.visit(*this);
        else
            throw Exception(
                    "accept(ValueSourceVisitor &) is not implemented for " + demangle(typeid(ConstSource<Base>).name())
                    + " because " + demangle(typeid(Base).name()) + " is not derived from IValueSource ");
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

    size_t getColumnSize() const
    {
        return total_rows;
    }

    bool isConst() const
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


struct GenericArraySource : public ArraySourceImpl<GenericArraySource>
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


template <typename ArraySource>
struct NullableArraySource : public ArraySource
{
    using Slice = NullableSlice<typename ArraySource::Slice>;
    using ArraySource::prev_offset;
    using ArraySource::row_num;
    using ArraySource::offsets;

    const NullMap & null_map;

    NullableArraySource(const ColumnArray & arr, const NullMap & null_map)
            : ArraySource(arr), null_map(null_map)
    {
    }

    void accept(ArraySourceVisitor & visitor) override { visitor.visit(*this); }

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

    bool isNullable() const override
    {
        return true;
    }
};


template <typename T>
struct NumericValueSource : ValueSourceImpl<NumericValueSource<T>>
{
    using Slice = NumericValueSlice<T>;
    using Column = ColumnVector<T>;

    const T * begin;
    size_t total_rows;
    size_t row_num = 0;

    explicit NumericValueSource(const Column & col)
    {
        const auto & container = col.getData();
        begin = container.data();
        total_rows = container.size();
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
        return total_rows;
    }

    Slice getWhole() const
    {
        Slice slice;
        slice.value = begin[row_num];
        return slice;
    }
};

struct GenericValueSource : public ValueSourceImpl<GenericValueSource>
{
    using Slice = GenericValueSlice;

    const IColumn * column;
    size_t total_rows;
    size_t row_num = 0;

    explicit GenericValueSource(const IColumn & col)
    {
        column = &col;
        total_rows = col.size();
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
        return total_rows;
    }

    Slice getWhole() const
    {
        Slice slice;
        slice.elements = column;
        slice.position = row_num;
        return slice;
    }
};

template <typename ValueSource>
struct NullableValueSource : public ValueSource
{
    using Slice = NullableSlice<typename ValueSource::Slice>;
    using ValueSource::row_num;

    const NullMap & null_map;

    template <typename Column>
    explicit NullableValueSource(const Column & col, const NullMap & null_map) : ValueSource(col), null_map(null_map) {}

    void accept(ValueSourceVisitor & visitor) override { visitor.visit(*this); }

    Slice getWhole() const
    {
        Slice slice = ValueSource::getWhole();
        slice.null_map = null_map.data() + row_num;
        return slice;
    }
};

}
