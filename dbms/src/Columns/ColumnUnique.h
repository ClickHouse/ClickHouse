#pragma once
#include <Columns/IColumnUnique.h>
#include <Columns/ReverseIndex.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NumberTraits.h>

#include <Common/typeid_cast.h>
#include <ext/range.h>

namespace DB
{


template <typename ColumnType>
class ColumnUnique final : public COWPtrHelper<IColumnUnique, ColumnUnique<ColumnType>>
{
    friend class COWPtrHelper<IColumnUnique, ColumnUnique<ColumnType>>;

private:
    explicit ColumnUnique(MutableColumnPtr && holder, bool is_nullable);
    explicit ColumnUnique(const IDataType & type);
    ColumnUnique(const ColumnUnique & other);

public:
    MutableColumnPtr cloneEmpty() const override;

    const ColumnPtr & getNestedColumn() const override;
    const ColumnPtr & getNestedNotNullableColumn() const override { return column_holder; }

    size_t uniqueInsert(const Field & x) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    MutableColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    IColumnUnique::IndexesWithOverflow uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length,
                                                                     size_t max_dictionary_size) override;
    size_t uniqueInsertData(const char * pos, size_t length) override;
    size_t uniqueInsertDataWithTerminatingZero(const char * pos, size_t length) override;
    size_t uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) override;

    size_t getDefaultValueIndex() const override { return is_nullable ? 1 : 0; }
    size_t getNullValueIndex() const override;
    bool canContainNulls() const override { return is_nullable; }

    Field operator[](size_t n) const override { return (*getNestedColumn())[n]; }
    void get(size_t n, Field & res) const override { getNestedColumn()->get(n, res); }
    StringRef getDataAt(size_t n) const override { return getNestedColumn()->getDataAt(n); }
    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        return getNestedColumn()->getDataAtWithTerminatingZero(n);
    }
    UInt64 get64(size_t n) const override { return getNestedColumn()->get64(n); }
    UInt64 getUInt(size_t n) const override { return getNestedColumn()->getUInt(n); }
    Int64 getInt(size_t n) const override { return getNestedColumn()->getInt(n); }
    bool isNullAt(size_t n) const override { return is_nullable && n == getNullValueIndex(); }
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        return column_holder->serializeValueIntoArena(n, arena, begin);
    }
    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        return getNestedColumn()->updateHashWithValue(n, hash);
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override
    {
        auto & column_unique = static_cast<const IColumnUnique&>(rhs);
        return getNestedColumn()->compareAt(n, m, *column_unique.getNestedColumn(), nan_direction_hint);
    }

    void getExtremes(Field & min, Field & max) const override { column_holder->getExtremes(min, max); }
    bool valuesHaveFixedSize() const override { return column_holder->valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return column_holder->isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return column_holder->sizeOfValueIfFixed(); }
    bool isNumeric() const override { return column_holder->isNumeric(); }

    size_t byteSize() const override { return column_holder->byteSize(); }
    size_t allocatedBytes() const override
    {
        return column_holder->allocatedBytes()
               + index.allocatedBytes()
               + (cached_null_mask ? cached_null_mask->allocatedBytes() : 0);
    }
    void forEachSubcolumn(IColumn::ColumnCallback callback) override
    {
        callback(column_holder);
        index.setColumn(getRawColumnPtr());
    }

private:

    ColumnPtr column_holder;
    bool is_nullable;
    ReverseIndex<UInt64, ColumnType> index;

    /// For DataTypeNullable, stores null map.
    mutable ColumnPtr cached_null_mask;
    mutable ColumnPtr cached_column_nullable;

    static size_t numSpecialValues(bool is_nullable) { return is_nullable ? 2 : 1; }
    size_t numSpecialValues() const { return numSpecialValues(is_nullable); }

    ColumnType * getRawColumnPtr() { return static_cast<ColumnType *>(column_holder->assumeMutable().get()); }
    const ColumnType * getRawColumnPtr() const { return static_cast<const ColumnType *>(column_holder.get()); }

    template <typename IndexType>
    MutableColumnPtr uniqueInsertRangeImpl(
        const IColumn & src,
        size_t start,
        size_t length,
        size_t num_added_rows,
        typename ColumnVector<IndexType>::MutablePtr && positions_column,
        ReverseIndex<UInt64, ColumnType> * secondary_index,
        size_t max_dictionary_size);
};

template <typename ColumnType>
MutableColumnPtr ColumnUnique<ColumnType>::cloneEmpty() const
{
    return ColumnUnique<ColumnType>::create(column_holder->cloneResized(numSpecialValues()), is_nullable);
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(const ColumnUnique & other)
    : column_holder(other.column_holder)
    , is_nullable(other.is_nullable)
    , index(numSpecialValues(is_nullable), 0)
{
    index.setColumn(getRawColumnPtr());
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(const IDataType & type)
    : is_nullable(type.isNullable())
    , index(numSpecialValues(is_nullable), 0)
{
    const auto & holder_type = is_nullable ? *static_cast<const DataTypeNullable &>(type).getNestedType() : type;
    column_holder = holder_type.createColumn()->cloneResized(numSpecialValues());
    index.setColumn(getRawColumnPtr());
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(MutableColumnPtr && holder, bool is_nullable)
    : column_holder(std::move(holder))
    , is_nullable(is_nullable)
    , index(numSpecialValues(is_nullable), 0)
{
    if (column_holder->size() < numSpecialValues())
        throw Exception("Too small holder column for ColumnUnique.", ErrorCodes::ILLEGAL_COLUMN);
    if (column_holder->isColumnNullable())
        throw Exception("Holder column for ColumnUnique can't be nullable.", ErrorCodes::ILLEGAL_COLUMN);

    index.setColumn(getRawColumnPtr());
}

template <typename ColumnType>
const ColumnPtr & ColumnUnique<ColumnType>::getNestedColumn() const
{
    if (is_nullable)
    {
        size_t size = getRawColumnPtr()->size();
        if (!cached_null_mask)
        {
            ColumnUInt8::MutablePtr null_mask = ColumnUInt8::create(size, UInt8(0));
            null_mask->getData()[getNullValueIndex()] = 1;
            cached_null_mask = std::move(null_mask);
            cached_column_nullable = ColumnNullable::create(column_holder, cached_null_mask);
        }

        if (cached_null_mask->size() != size)
        {
            MutableColumnPtr null_mask = (*std::move(cached_null_mask)).mutate();
            static_cast<ColumnUInt8 &>(*null_mask).getData().resize_fill(size);
            cached_null_mask = std::move(null_mask);
            cached_column_nullable = ColumnNullable::create(column_holder, cached_null_mask);
        }

        return cached_column_nullable;
    }
    return column_holder;
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::getNullValueIndex() const
{
    if (!is_nullable)
        throw Exception("ColumnUnique can't contain null values.", ErrorCodes::LOGICAL_ERROR);

    return 0;
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsert(const Field & x)
{
    if (x.getType() == Field::Types::Null)
        return getNullValueIndex();

    auto column = getRawColumnPtr();
    auto prev_size = static_cast<UInt64>(column->size());

    if ((*column)[getDefaultValueIndex()] == x)
        return getDefaultValueIndex();

    column->insert(x);
    auto pos = index.insert(prev_size);
    if (pos != prev_size)
        column->popBack(1);

    return pos;
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertFrom(const IColumn & src, size_t n)
{
    if (is_nullable && src.isNullAt(n))
        return getNullValueIndex();

    if (auto * nullable = typeid_cast<const ColumnNullable *>(&src))
        return uniqueInsertFrom(nullable->getNestedColumn(), n);

    auto ref = src.getDataAt(n);
    return uniqueInsertData(ref.data, ref.size);
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertData(const char * pos, size_t length)
{
    auto column = getRawColumnPtr();

    if (column->getDataAt(getDefaultValueIndex()) == StringRef(pos, length))
        return getDefaultValueIndex();

    UInt64 size = column->size();
    UInt64 insertion_point = index.getInsertionPoint(StringRef(pos, length));

    if (insertion_point == size)
    {
        column->insertData(pos, length);
        index.insertFromLastRow();
    }

    return insertion_point;
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertDataWithTerminatingZero(const char * pos, size_t length)
{
    if (std::is_same<ColumnType, ColumnString>::value)
        return uniqueInsertData(pos, length - 1);

    if (column_holder->valuesHaveFixedSize())
        return uniqueInsertData(pos, length);

    /// Don't know if data actually has terminating zero. So, insert it firstly.

    auto column = getRawColumnPtr();
    size_t prev_size = column->size();
    column->insertDataWithTerminatingZero(pos, length);

    if (column->compareAt(getDefaultValueIndex(), prev_size, *column, 1) == 0)
    {
        column->popBack(1);
        return getDefaultValueIndex();
    }

    auto position = index.insert(prev_size);
    if (position != prev_size)
        column->popBack(1);

    return static_cast<size_t>(position);
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos)
{
    auto column = getRawColumnPtr();
    size_t prev_size = column->size();
    new_pos = column->deserializeAndInsertFromArena(pos);

    if (column->compareAt(getDefaultValueIndex(), prev_size, *column, 1) == 0)
    {
        column->popBack(1);
        return getDefaultValueIndex();
    }

    auto index_pos = index.insert(prev_size);
    if (index_pos != prev_size)
        column->popBack(1);

    return static_cast<size_t>(index_pos);
}

template <typename IndexType>
static void checkIndexes(const ColumnVector<IndexType> & indexes, size_t max_dictionary_size)
{
    auto & data = indexes.getData();
    for (size_t i = 0; i < data.size(); ++i)
    {
        if (data[i] >= max_dictionary_size)
        {
            throw Exception("Found index " + toString(data[i]) + " at position " + toString(i)
                            + " which is grated or equal than dictionary size " + toString(max_dictionary_size),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }
}

template <typename ColumnType>
template <typename IndexType>
MutableColumnPtr ColumnUnique<ColumnType>::uniqueInsertRangeImpl(
    const IColumn & src,
    size_t start,
    size_t length,
    size_t num_added_rows,
    typename ColumnVector<IndexType>::MutablePtr && positions_column,
    ReverseIndex<UInt64, ColumnType> * secondary_index,
    size_t max_dictionary_size)
{
    const ColumnType * src_column;
    const NullMap * null_map = nullptr;
    auto & positions = positions_column->getData();

    auto update_position = [&](UInt64 & next_position) -> MutableColumnPtr
    {
        constexpr auto next_size = NumberTraits::nextSize(sizeof(IndexType));
        using SuperiorIndexType = typename NumberTraits::Construct<false, false, next_size>::Type;

        ++next_position;

        if (next_position > std::numeric_limits<IndexType>::max())
        {
            if (sizeof(SuperiorIndexType) == sizeof(IndexType))
                throw Exception("Can't find superior index type for type " + demangle(typeid(IndexType).name()),
                                ErrorCodes::LOGICAL_ERROR);

            auto expanded_column = ColumnVector<SuperiorIndexType>::create(length);
            auto & expanded_data = expanded_column->getData();
            for (size_t i = 0; i < num_added_rows; ++i)
                expanded_data[i] = positions[i];

            return uniqueInsertRangeImpl<SuperiorIndexType>(
                    src,
                    start,
                    length,
                    num_added_rows,
                    std::move(expanded_column),
                    secondary_index,
                    max_dictionary_size);
        }

        return nullptr;
    };

    if (auto nullable_column = typeid_cast<const ColumnNullable *>(&src))
    {
        src_column = typeid_cast<const ColumnType *>(&nullable_column->getNestedColumn());
        null_map = &nullable_column->getNullMapData();
    }
    else
        src_column = typeid_cast<const ColumnType *>(&src);

    if (src_column == nullptr)
        throw Exception("Invalid column type for ColumnUnique::insertRangeFrom. Expected " + column_holder->getName() +
                        ", got " + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    auto column = getRawColumnPtr();

    UInt64 next_position = column->size();
    if (secondary_index)
        next_position += secondary_index->size();

    auto check_inserted_position = [&next_position](UInt64 inserted_position)
    {
        if (inserted_position != next_position)
            throw Exception("Inserted position " + toString(inserted_position)
                            + " is not equal with expected " + toString(next_position), ErrorCodes::LOGICAL_ERROR);
    };

    auto insert_key = [&](const StringRef & ref, ReverseIndex<UInt64, ColumnType> * cur_index)
    {
        positions[num_added_rows] = next_position;
        cur_index->getColumn()->insertData(ref.data, ref.size);
        auto inserted_pos = cur_index->insertFromLastRow();
        check_inserted_position(inserted_pos);
        return update_position(next_position);
    };

    for (; num_added_rows < length; ++num_added_rows)
    {
        auto row = start + num_added_rows;

        if (null_map && (*null_map)[row])
            positions[num_added_rows] = getNullValueIndex();
        else if (column->compareAt(getDefaultValueIndex(), row, *src_column, 1) == 0)
            positions[num_added_rows] = getDefaultValueIndex();
        else
        {
            auto ref = src_column->getDataAt(row);
            auto cur_index = &index;
            bool inserted = false;

            while (!inserted)
            {
                auto insertion_point = cur_index->getInsertionPoint(ref);

                if (insertion_point == cur_index->lastInsertionPoint())
                {
                    if (secondary_index && cur_index != secondary_index && next_position >= max_dictionary_size)
                    {
                        cur_index = secondary_index;
                        continue;
                    }

                    if (auto res = insert_key(ref, cur_index))
                        return res;
                }
                else
                   positions[num_added_rows] = insertion_point;

                inserted = true;
            }
        }
    }

    // checkIndexes(*positions_column, column->size() + (overflowed_keys ? overflowed_keys->size() : 0));
    return std::move(positions_column);
}

template <typename ColumnType>
MutableColumnPtr ColumnUnique<ColumnType>::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    auto callForType = [this, &src, start, length](auto x) -> MutableColumnPtr
    {
        size_t size = getRawColumnPtr()->size();

        using IndexType = decltype(x);
        if (size <= std::numeric_limits<IndexType>::max())
        {
            auto positions = ColumnVector<IndexType>::create(length);
            return this->uniqueInsertRangeImpl<IndexType>(src, start, length, 0, std::move(positions), nullptr, 0);
        }

        return nullptr;
    };

    MutableColumnPtr positions_column;
    if (!positions_column)
        positions_column = callForType(UInt8());
    if (!positions_column)
        positions_column = callForType(UInt16());
    if (!positions_column)
        positions_column = callForType(UInt32());
    if (!positions_column)
        positions_column = callForType(UInt64());
    if (!positions_column)
        throw Exception("Can't find index type for ColumnUnique", ErrorCodes::LOGICAL_ERROR);

    return positions_column;
}

template <typename ColumnType>
IColumnUnique::IndexesWithOverflow ColumnUnique<ColumnType>::uniqueInsertRangeWithOverflow(
    const IColumn & src,
    size_t start,
    size_t length,
    size_t max_dictionary_size)
{
    auto overflowed_keys = column_holder->cloneEmpty();
    auto overflowed_keys_ptr = typeid_cast<ColumnType *>(overflowed_keys.get());
    if (!overflowed_keys_ptr)
        throw Exception("Invalid keys type for ColumnUnique.", ErrorCodes::LOGICAL_ERROR);

    auto callForType = [this, &src, start, length, overflowed_keys_ptr, max_dictionary_size](auto x) -> MutableColumnPtr
    {
        size_t size = getRawColumnPtr()->size();

        using IndexType = decltype(x);
        if (size <= std::numeric_limits<IndexType>::max())
        {
            auto positions = ColumnVector<IndexType>::create(length);
            ReverseIndex<UInt64, ColumnType> secondary_index(0, max_dictionary_size);
            secondary_index.setColumn(overflowed_keys_ptr);
            return this->uniqueInsertRangeImpl<IndexType>(src, start, length, 0, std::move(positions),
                                                          &secondary_index, max_dictionary_size);
        }

        return nullptr;
    };

    MutableColumnPtr positions_column;
    if (!positions_column)
        positions_column = callForType(UInt8());
    if (!positions_column)
        positions_column = callForType(UInt16());
    if (!positions_column)
        positions_column = callForType(UInt32());
    if (!positions_column)
        positions_column = callForType(UInt64());
    if (!positions_column)
        throw Exception("Can't find index type for ColumnUnique", ErrorCodes::LOGICAL_ERROR);

    IColumnUnique::IndexesWithOverflow indexes_with_overflow;
    indexes_with_overflow.indexes = std::move(positions_column);
    indexes_with_overflow.overflowed_keys = std::move(overflowed_keys);
    return indexes_with_overflow;
}

};
