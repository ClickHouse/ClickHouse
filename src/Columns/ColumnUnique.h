#pragma once
#include <Columns/IColumnUnique.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ReverseIndex.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NumberTraits.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <ext/range.h>

#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

template <typename ColumnType>
class ColumnUnique final : public COWHelper<IColumnUnique, ColumnUnique<ColumnType>>
{
    friend class COWHelper<IColumnUnique, ColumnUnique<ColumnType>>;

private:
    explicit ColumnUnique(MutableColumnPtr && holder, bool is_nullable);
    explicit ColumnUnique(const IDataType & type);
    ColumnUnique(const ColumnUnique & other);

public:
    MutableColumnPtr cloneEmpty() const override;

    const ColumnPtr & getNestedColumn() const override;
    const ColumnPtr & getNestedNotNullableColumn() const override { return column_holder; }
    bool nestedColumnIsNullable() const override { return is_nullable; }

    size_t uniqueInsert(const Field & x) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    MutableColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    IColumnUnique::IndexesWithOverflow uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length,
                                                                     size_t max_dictionary_size) override;
    size_t uniqueInsertData(const char * pos, size_t length) override;
    size_t uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) override;

    size_t getDefaultValueIndex() const override { return 0; }
    size_t getNullValueIndex() const override;
    size_t getNestedTypeDefaultValueIndex() const override { return is_nullable ? 1 : 0; }
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
    Float64 getFloat64(size_t n) const override { return getNestedColumn()->getFloat64(n); }
    Float32 getFloat32(size_t n) const override { return getNestedColumn()->getFloat32(n); }
    bool getBool(size_t n) const override { return getNestedColumn()->getBool(n); }
    bool isNullAt(size_t n) const override { return is_nullable && n == getNullValueIndex(); }
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    void updateHashWithValue(size_t n, SipHash & hash_func) const override
    {
        return getNestedColumn()->updateHashWithValue(n, hash_func);
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_range) const override;

    void getExtremes(Field & min, Field & max) const override { column_holder->getExtremes(min, max); }
    bool valuesHaveFixedSize() const override { return column_holder->valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return column_holder->isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return column_holder->sizeOfValueIfFixed(); }
    bool isNumeric() const override { return column_holder->isNumeric(); }

    size_t byteSize() const override { return column_holder->byteSize(); }
    void protect() override { column_holder->protect(); }
    size_t allocatedBytes() const override
    {
        return column_holder->allocatedBytes()
               + index.allocatedBytes()
               + (nested_null_mask ? nested_null_mask->allocatedBytes() : 0);
    }
    void forEachSubcolumn(IColumn::ColumnCallback callback) override
    {
        callback(column_holder);
        index.setColumn(getRawColumnPtr());
        if (is_nullable)
            nested_column_nullable = ColumnNullable::create(column_holder, nested_null_mask);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (auto rhs_concrete = typeid_cast<const ColumnUnique *>(&rhs))
            return column_holder->structureEquals(*rhs_concrete->column_holder);
        return false;
    }

    const UInt64 * tryGetSavedHash() const override { return index.tryGetSavedHash(); }

    UInt128 getHash() const override { return hash.getHash(*getRawColumnPtr()); }

private:

    IColumn::WrappedPtr column_holder;
    bool is_nullable;
    size_t size_of_value_if_fixed = 0;
    ReverseIndex<UInt64, ColumnType> index;

    /// For DataTypeNullable, stores null map.
    IColumn::WrappedPtr nested_null_mask;
    IColumn::WrappedPtr nested_column_nullable;

    class IncrementalHash
    {
    private:
        UInt128 hash;
        std::atomic<size_t> num_added_rows;

        std::mutex mutex;
    public:
        IncrementalHash() : num_added_rows(0) {}

        UInt128 getHash(const ColumnType & column);
    };

    mutable IncrementalHash hash;

    void createNullMask();
    void updateNullMask();

    static size_t numSpecialValues(bool is_nullable) { return is_nullable ? 2 : 1; }
    size_t numSpecialValues() const { return numSpecialValues(is_nullable); }

    ColumnType * getRawColumnPtr() { return assert_cast<ColumnType *>(column_holder.get()); }
    const ColumnType * getRawColumnPtr() const { return assert_cast<const ColumnType *>(column_holder.get()); }

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
    , size_of_value_if_fixed (other.size_of_value_if_fixed)
    , index(numSpecialValues(is_nullable), 0)
{
    index.setColumn(getRawColumnPtr());
    createNullMask();
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(const IDataType & type)
    : is_nullable(type.isNullable())
    , index(numSpecialValues(is_nullable), 0)
{
    const auto & holder_type = is_nullable ? *static_cast<const DataTypeNullable &>(type).getNestedType() : type;
    column_holder = holder_type.createColumn()->cloneResized(numSpecialValues());
    index.setColumn(getRawColumnPtr());
    createNullMask();

    if (column_holder->valuesHaveFixedSize())
        size_of_value_if_fixed = column_holder->sizeOfValueIfFixed();
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(MutableColumnPtr && holder, bool is_nullable_)
    : column_holder(std::move(holder))
    , is_nullable(is_nullable_)
    , index(numSpecialValues(is_nullable_), 0)
{
    if (column_holder->size() < numSpecialValues())
        throw Exception("Too small holder column for ColumnUnique.", ErrorCodes::ILLEGAL_COLUMN);
    if (isColumnNullable(*column_holder))
        throw Exception("Holder column for ColumnUnique can't be nullable.", ErrorCodes::ILLEGAL_COLUMN);

    index.setColumn(getRawColumnPtr());
    createNullMask();

    if (column_holder->valuesHaveFixedSize())
        size_of_value_if_fixed = column_holder->sizeOfValueIfFixed();
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::createNullMask()
{
    if (is_nullable)
    {
        size_t size = getRawColumnPtr()->size();
        if (!nested_null_mask)
        {
            ColumnUInt8::MutablePtr null_mask = ColumnUInt8::create(size, UInt8(0));
            null_mask->getData()[getNullValueIndex()] = 1;
            nested_null_mask = std::move(null_mask);
            nested_column_nullable = ColumnNullable::create(column_holder, nested_null_mask);
        }
        else
            throw Exception("Null mask for ColumnUnique is already created.", ErrorCodes::LOGICAL_ERROR);
    }
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::updateNullMask()
{
    if (is_nullable)
    {
        if (!nested_null_mask)
            throw Exception("Null mask for ColumnUnique is was not created.", ErrorCodes::LOGICAL_ERROR);

        size_t size = getRawColumnPtr()->size();

        if (nested_null_mask->size() != size)
            assert_cast<ColumnUInt8 &>(*nested_null_mask).getData().resize_fill(size);
    }
}

template <typename ColumnType>
const ColumnPtr & ColumnUnique<ColumnType>::getNestedColumn() const
{
    if (is_nullable)
        return nested_column_nullable;

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

    if (isNumeric())
        return uniqueInsertData(&x.reinterpret<char>(), size_of_value_if_fixed);

    auto & val = x.get<String>();
    return uniqueInsertData(val.data(), val.size());
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertFrom(const IColumn & src, size_t n)
{
    if (is_nullable && src.isNullAt(n))
        return getNullValueIndex();

    if (auto * nullable = checkAndGetColumn<ColumnNullable>(src))
        return uniqueInsertFrom(nullable->getNestedColumn(), n);

    auto ref = src.getDataAt(n);
    return uniqueInsertData(ref.data, ref.size);
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertData(const char * pos, size_t length)
{
    auto column = getRawColumnPtr();

    if (column->getDataAt(getNestedTypeDefaultValueIndex()) == StringRef(pos, length))
        return getNestedTypeDefaultValueIndex();

    auto insertion_point = index.insert(StringRef(pos, length));

    updateNullMask();

    return insertion_point;
}

template <typename ColumnType>
StringRef ColumnUnique<ColumnType>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    if (is_nullable)
    {
        static constexpr auto s = sizeof(UInt8);

        auto pos = arena.allocContinue(s, begin);
        UInt8 flag = (n == getNullValueIndex() ? 1 : 0);
        unalignedStore<UInt8>(pos, flag);

        if (n == getNullValueIndex())
            return StringRef(pos, s);

        auto nested_ref = column_holder->serializeValueIntoArena(n, arena, begin);

        /// serializeValueIntoArena may reallocate memory. Have to use ptr from nested_ref.data and move it back.
        return StringRef(nested_ref.data - s, nested_ref.size + s);
    }

    return column_holder->serializeValueIntoArena(n, arena, begin);
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos)
{
    if (is_nullable)
    {
        UInt8 val = unalignedLoad<UInt8>(pos);
        pos += sizeof(val);

        if (val)
        {
            new_pos = pos;
            return getNullValueIndex();
        }
    }

    /// Numbers, FixedString
    if (size_of_value_if_fixed)
    {
        new_pos = pos + size_of_value_if_fixed;
        return uniqueInsertData(pos, size_of_value_if_fixed);
    }

    /// String
    const size_t string_size = unalignedLoad<size_t>(pos);
    pos += sizeof(string_size);
    new_pos = pos + string_size;

    /// -1 because of terminating zero
    return uniqueInsertData(pos, string_size - 1);
}

template <typename ColumnType>
int ColumnUnique<ColumnType>::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    if (is_nullable)
    {
        /// See ColumnNullable::compareAt
        bool lval_is_null = n == getNullValueIndex();
        bool rval_is_null = m == getNullValueIndex();

        if (unlikely(lval_is_null || rval_is_null))
        {
            if (lval_is_null && rval_is_null)
                return 0;
            else
                return lval_is_null ? nan_direction_hint : -nan_direction_hint;
        }
    }

    auto & column_unique = static_cast<const IColumnUnique &>(rhs);
    return getNestedColumn()->compareAt(n, m, *column_unique.getNestedColumn(), nan_direction_hint);
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_range) const
{
    bool found_null_value_index = false;
    for (size_t i = 0; i < equal_range.size() && !found_null_value_index; ++i)
    {
        auto& [first, last] = equal_range[i];
        for (auto j = first; j < last; ++j)
        {
            if (res[j] == getNullValueIndex())
            {
                if ((nan_direction_hint > 0) != reverse)
                {
                    std::swap(res[j], res[last - 1]);
                    --last;
                }
                else
                {
                    std::swap(res[j], res[first]);
                    ++first;
                }
                if (last - first <= 1)
                {
                    equal_range.erase(equal_range.begin() + i);
                }
                found_null_value_index = true;
                break;
            }
        }
    }
    getNestedColumn()->updatePermutation(reverse, limit, nan_direction_hint, res, equal_range);
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

    if (auto * nullable_column = checkAndGetColumn<ColumnNullable>(src))
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

    auto insert_key = [&](const StringRef & ref, ReverseIndex<UInt64, ColumnType> & cur_index) -> MutableColumnPtr
    {
        auto inserted_pos = cur_index.insert(ref);
        positions[num_added_rows] = inserted_pos;
        if (inserted_pos == next_position)
            return update_position(next_position);

        return nullptr;
    };

    for (; num_added_rows < length; ++num_added_rows)
    {
        auto row = start + num_added_rows;

        if (null_map && (*null_map)[row])
            positions[num_added_rows] = getNullValueIndex();
        else if (column->compareAt(getNestedTypeDefaultValueIndex(), row, *src_column, 1) == 0)
            positions[num_added_rows] = getNestedTypeDefaultValueIndex();
        else
        {
            auto ref = src_column->getDataAt(row);
            MutableColumnPtr res = nullptr;

            if (secondary_index && next_position >= max_dictionary_size)
            {
                auto insertion_point = index.getInsertionPoint(ref);
                if (insertion_point == index.lastInsertionPoint())
                    res = insert_key(ref, *secondary_index);
                else
                    positions[num_added_rows] = insertion_point;
            }
            else
                res = insert_key(ref, index);

            if (res)
                return res;
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

    updateNullMask();

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

    updateNullMask();

    IColumnUnique::IndexesWithOverflow indexes_with_overflow;
    indexes_with_overflow.indexes = std::move(positions_column);
    indexes_with_overflow.overflowed_keys = std::move(overflowed_keys);
    return indexes_with_overflow;
}

template <typename ColumnType>
UInt128 ColumnUnique<ColumnType>::IncrementalHash::getHash(const ColumnType & column)
{
    size_t column_size = column.size();
    UInt128 cur_hash;

    if (column_size != num_added_rows.load())
    {
        SipHash sip_hash;
        for (size_t i = 0; i < column_size; ++i)
            column.updateHashWithValue(i, sip_hash);

        std::lock_guard lock(mutex);
        sip_hash.get128(hash.low, hash.high);
        cur_hash = hash;
        num_added_rows.store(column_size);
    }
    else
    {
        std::lock_guard lock(mutex);
        cur_hash = hash;
    }

    return cur_hash;
}

}
