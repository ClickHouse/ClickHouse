#pragma once

#include <Columns/IColumnUnique.h>
#include <Columns/ReverseIndex.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NumberTraits.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>

#include <base/range.h>
#include <base/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

/** Stores another column with unique values
  * and also an index that allows to find position by value.
  *
  * This column is not used on it's own but only as implementation detail of ColumnLowCardinality.
  */
template <typename ColumnType>
class ColumnUnique final : public COWHelper<IColumnUnique, ColumnUnique<ColumnType>>
{
    friend class COWHelper<IColumnUnique, ColumnUnique<ColumnType>>;

private:
    ColumnUnique(MutableColumnPtr && holder, bool is_nullable);
    explicit ColumnUnique(const IDataType & type);
    ColumnUnique(const ColumnUnique & other);

public:
    std::string getName() const override { return "Unique(" + getNestedColumn()->getName() + ")"; }

    MutableColumnPtr cloneEmpty() const override;

    const ColumnPtr & getNestedColumn() const override;
    const ColumnPtr & getNestedNotNullableColumn() const override { return column_holder; }
    bool nestedColumnIsNullable() const override { return is_nullable; }
    void nestedToNullable() override;
    void nestedRemoveNullable() override;

    size_t uniqueInsert(const Field & x) override;
    bool tryUniqueInsert(const Field & x, size_t & index) override;
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
    std::pair<String, DataTypePtr> getValueNameAndType(size_t n) const override
    {
        return getNestedColumn()->getValueNameAndType(n);
    }
    bool isDefaultAt(size_t n) const override { return n == 0; }
    StringRef getDataAt(size_t n) const override { return getNestedColumn()->getDataAt(n); }
    UInt64 get64(size_t n) const override { return getNestedColumn()->get64(n); }
    UInt64 getUInt(size_t n) const override { return getNestedColumn()->getUInt(n); }
    Int64 getInt(size_t n) const override { return getNestedColumn()->getInt(n); }
    Float64 getFloat64(size_t n) const override { return getNestedColumn()->getFloat64(n); }
    Float32 getFloat32(size_t n) const override { return getNestedColumn()->getFloat32(n); }
    bool getBool(size_t n) const override { return getNestedColumn()->getBool(n); }
    bool isNullAt(size_t n) const override { return is_nullable && n == getNullValueIndex(); }
    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash_func) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    void getExtremes(Field & min, Field & max) const override { column_holder->getExtremes(min, max); }
    bool valuesHaveFixedSize() const override { return column_holder->valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return column_holder->isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return column_holder->sizeOfValueIfFixed(); }
    bool isNumeric() const override { return column_holder->isNumeric(); }

    size_t byteSize() const override { return column_holder->byteSize(); }
    size_t byteSizeAt(size_t n) const override
    {
        return getNestedColumn()->byteSizeAt(n);
    }
    void protect() override { column_holder->protect(); }
    size_t allocatedBytes() const override
    {
        return column_holder->allocatedBytes() + reverse_index.allocatedBytes()
            + (nested_null_mask ? nested_null_mask->allocatedBytes() : 0);
    }

    void forEachSubcolumn(IColumn::ColumnCallback callback) const override
    {
        callback(column_holder);
    }

    void forEachMutableSubcolumn(IColumn::MutableColumnCallback callback) override
    {
        callback(column_holder);
        reverse_index.setColumn(getRawColumnPtr());
        if (is_nullable)
            nested_column_nullable = ColumnNullable::create(column_holder, nested_null_mask);
    }

    void forEachSubcolumnRecursively(IColumn::RecursiveColumnCallback callback) const override
    {
        callback(*column_holder);
        column_holder->forEachSubcolumnRecursively(callback);
    }

    void forEachMutableSubcolumnRecursively(IColumn::RecursiveMutableColumnCallback callback) override
    {
        callback(*column_holder);
        column_holder->forEachMutableSubcolumnRecursively(callback);
        reverse_index.setColumn(getRawColumnPtr());
        if (is_nullable)
            nested_column_nullable = ColumnNullable::create(column_holder, nested_null_mask);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (auto rhs_concrete = typeid_cast<const ColumnUnique *>(&rhs))
            return column_holder->structureEquals(*rhs_concrete->column_holder);
        return false;
    }

    double getRatioOfDefaultRows(double) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getRatioOfDefaultRows' not implemented for ColumnUnique");
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getNumberOfDefaultRows' not implemented for ColumnUnique");
    }

    void getIndicesOfNonDefaultRows(IColumn::Offsets &, size_t, size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getIndicesOfNonDefaultRows' not implemented for ColumnUnique");
    }

    const UInt64 * tryGetSavedHash() const override { return reverse_index.tryGetSavedHash(); }

    UInt128 getHash() const override { return hash.getHash(*getRawColumnPtr()); }

    /// This is strange. Please remove this method as soon as possible.
    std::optional<UInt64> getOrFindValueIndex(StringRef value) const override
    {
        if (std::optional<UInt64> res = reverse_index.getIndex(value); res)
            return res;

        const IColumn & nested = *getNestedColumn();

        for (size_t i = 0; i < nested.size(); ++i)
            if (!nested.isNullAt(i) && nested.getDataAt(i) == value)
                return i;

        return {};
    }

private:
    IColumn::WrappedPtr column_holder;
    bool is_nullable;
    size_t size_of_value_if_fixed = 0;

    ReverseIndex<UInt64, ColumnType> reverse_index;

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
    , size_of_value_if_fixed(other.size_of_value_if_fixed)
    , reverse_index(numSpecialValues(is_nullable), 0)
{
    reverse_index.setColumn(getRawColumnPtr());
    createNullMask();
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(const IDataType & type)
    : is_nullable(type.isNullable()), reverse_index(numSpecialValues(is_nullable), 0)
{
    const auto & holder_type = is_nullable ? *static_cast<const DataTypeNullable &>(type).getNestedType() : type;
    column_holder = holder_type.createColumn()->cloneResized(numSpecialValues());
    reverse_index.setColumn(getRawColumnPtr());
    createNullMask();

    if (column_holder->valuesHaveFixedSize())
        size_of_value_if_fixed = column_holder->sizeOfValueIfFixed();
}

template <typename ColumnType>
ColumnUnique<ColumnType>::ColumnUnique(MutableColumnPtr && holder, bool is_nullable_)
    : column_holder(std::move(holder)), is_nullable(is_nullable_), reverse_index(numSpecialValues(is_nullable_), 0)
{
    if (column_holder->size() < numSpecialValues())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Too small holder column for ColumnUnique.");
    if (isColumnNullable(*column_holder))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Holder column for ColumnUnique can't be nullable.");

    reverse_index.setColumn(getRawColumnPtr());
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Null mask for ColumnUnique is already created.");
    }
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::updateNullMask()
{
    if (is_nullable)
    {
        if (!nested_null_mask)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Null mask for ColumnUnique is was not created.");

        size_t size = getRawColumnPtr()->size();

        if (nested_null_mask->size() != size)
            assert_cast<ColumnUInt8 &>(*nested_null_mask).getData().resize_fill(size);
    }
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::nestedToNullable()
{
    is_nullable = true;
    createNullMask();
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::nestedRemoveNullable()
{
    is_nullable = false;
    nested_null_mask = nullptr;
    nested_column_nullable = nullptr;
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnUnique can't contain null values.");

    return 0;
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsert(const Field & x)
{
    if (x.isNull())
        return getNullValueIndex();

    auto single_value_column = column_holder->cloneEmpty();
    single_value_column->insert(x);
    auto single_value_data = single_value_column->getDataAt(0);

    return uniqueInsertData(single_value_data.data, single_value_data.size);
}

template <typename ColumnType>
bool ColumnUnique<ColumnType>::tryUniqueInsert(const Field & x, size_t & index)
{
    if (x.isNull())
    {
        if (!is_nullable)
            return false;
        index = getNullValueIndex();
        return true;
    }

    auto single_value_column = column_holder->cloneEmpty();
    if (!single_value_column->tryInsert(x))
        return false;

    auto single_value_data = single_value_column->getDataAt(0);
    index = uniqueInsertData(single_value_data.data, single_value_data.size);
    return true;
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertFrom(const IColumn & src, size_t n)
{
    if (is_nullable && src.isNullAt(n))
        return getNullValueIndex();

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&src))
        return uniqueInsertFrom(nullable->getNestedColumn(), n);

    auto ref = src.getDataAt(n);
    return uniqueInsertData(ref.data, ref.size);
}

template <typename ColumnType>
size_t ColumnUnique<ColumnType>::uniqueInsertData(const char * pos, size_t length)
{
    if (auto index = getNestedTypeDefaultValueIndex(); getRawColumnPtr()->getDataAt(index) == StringRef(pos, length))
        return index;

    auto insertion_point = reverse_index.insert({pos, length});

    updateNullMask();

    return insertion_point;
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const
{
    /// nullable is handled internally.
    chassert(is_null == nullptr);
    if (IColumn::empty())
        return;

    if (is_nullable)
        column_holder->collectSerializedValueSizes(sizes, assert_cast<const ColumnUInt8 &>(*nested_null_mask).getData().data());
    else
        column_holder->collectSerializedValueSizes(sizes, nullptr);
}

template <typename ColumnType>
StringRef ColumnUnique<ColumnType>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    if (is_nullable)
    {
        static constexpr auto s = sizeof(UInt8);

        auto * pos = arena.allocContinue(s, begin);
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
char * ColumnUnique<ColumnType>::serializeValueIntoMemory(size_t n, char * memory) const
{
    if (is_nullable)
    {
        UInt8 flag = (n == getNullValueIndex() ? 1 : 0);
        unalignedStore<UInt8>(memory, flag);
        ++memory;

        if (n == getNullValueIndex())
            return memory;
    }

    return column_holder->serializeValueIntoMemory(n, memory);
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
const char * ColumnUnique<ColumnType>::skipSerializedInArena(const char *) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method skipSerializedInArena is not supported for {}", this->getName());
}

template <typename ColumnType>
#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnUnique<ColumnType>::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnUnique<ColumnType>::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
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
            return lval_is_null ? nan_direction_hint : -nan_direction_hint;
        }
    }

    const auto & column_unique = static_cast<const IColumnUnique &>(rhs);
    return getNestedColumn()->compareAt(n, m, *column_unique.getNestedColumn(), nan_direction_hint);
}

template <typename IndexType>
static void checkIndexes(const ColumnVector<IndexType> & indexes, size_t max_dictionary_size)
{
    auto & data = indexes.getData();
    for (size_t i = 0; i < data.size(); ++i)
    {
        if (data[i] >= max_dictionary_size)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Found index {} at position {} which is grated or equal "
                            "than dictionary size {}", toString(data[i]), toString(i), toString(max_dictionary_size));
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find superior index type for type {}",
                                demangle(typeid(IndexType).name()));

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

    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&src))
    {
        src_column = typeid_cast<const ColumnType *>(&nullable_column->getNestedColumn());
        null_map = &nullable_column->getNullMapData();
    }
    else
        src_column = typeid_cast<const ColumnType *>(&src);

    if (src_column == nullptr)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Invalid column type for ColumnUnique::insertRangeFrom. "
                        "Expected {}, got {}", column_holder->getName(), src.getName());

    auto column = getRawColumnPtr();

    UInt64 next_position = column->size();
    if (secondary_index)
        next_position += secondary_index->size();

    auto insert_key = [&](StringRef ref, ReverseIndex<UInt64, ColumnType> & cur_index) -> MutableColumnPtr
    {
        auto inserted_pos = cur_index.insert(ref);
        positions[num_added_rows] = static_cast<IndexType>(inserted_pos);
        if (inserted_pos == next_position)
            return update_position(next_position);

        return nullptr;
    };

    for (; num_added_rows < length; ++num_added_rows)
    {
        auto row = start + num_added_rows;

        if (null_map && (*null_map)[row])
            positions[num_added_rows] = static_cast<IndexType>(getNullValueIndex());
        else if (column->compareAt(getNestedTypeDefaultValueIndex(), row, *src_column, 1) == 0)
            positions[num_added_rows] = static_cast<IndexType>(getNestedTypeDefaultValueIndex());
        else
        {
            auto ref = src_column->getDataAt(row);
            MutableColumnPtr res = nullptr;

            if (secondary_index && next_position >= max_dictionary_size)
            {
                auto insertion_point = reverse_index.getInsertionPoint(ref);
                if (insertion_point == reverse_index.lastInsertionPoint())
                    res = insert_key(ref, *secondary_index);
                else
                    positions[num_added_rows] = static_cast<IndexType>(insertion_point);
            }
            else
                res = insert_key(ref, reverse_index);

            if (res)
                return res;
        }
    }

    return std::move(positions_column);
}

template <typename ColumnType>
MutableColumnPtr ColumnUnique<ColumnType>::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    auto call_for_type = [this, &src, start, length](auto x) -> MutableColumnPtr
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
        positions_column = call_for_type(UInt8());
    if (!positions_column)
        positions_column = call_for_type(UInt16());
    if (!positions_column)
        positions_column = call_for_type(UInt32());
    if (!positions_column)
        positions_column = call_for_type(UInt64());
    if (!positions_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find index type for ColumnUnique");

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
    auto * overflowed_keys_ptr = typeid_cast<ColumnType *>(overflowed_keys.get());
    if (!overflowed_keys_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid keys type for ColumnUnique.");

    auto call_for_type = [this, &src, start, length, overflowed_keys_ptr, max_dictionary_size](auto x) -> MutableColumnPtr
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
        positions_column = call_for_type(UInt8());
    if (!positions_column)
        positions_column = call_for_type(UInt16());
    if (!positions_column)
        positions_column = call_for_type(UInt32());
    if (!positions_column)
        positions_column = call_for_type(UInt64());
    if (!positions_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find index type for ColumnUnique");

    updateNullMask();

    IColumnUnique::IndexesWithOverflow indexes_with_overflow;
    indexes_with_overflow.indexes = std::move(positions_column);
    indexes_with_overflow.overflowed_keys = std::move(overflowed_keys);
    return indexes_with_overflow;
}

extern template class ColumnUnique<ColumnInt8>;
extern template class ColumnUnique<ColumnUInt8>;
extern template class ColumnUnique<ColumnInt16>;
extern template class ColumnUnique<ColumnUInt16>;
extern template class ColumnUnique<ColumnInt32>;
extern template class ColumnUnique<ColumnUInt32>;
extern template class ColumnUnique<ColumnInt64>;
extern template class ColumnUnique<ColumnUInt64>;
extern template class ColumnUnique<ColumnInt128>;
extern template class ColumnUnique<ColumnUInt128>;
extern template class ColumnUnique<ColumnInt256>;
extern template class ColumnUnique<ColumnUInt256>;
extern template class ColumnUnique<ColumnBFloat16>;
extern template class ColumnUnique<ColumnFloat32>;
extern template class ColumnUnique<ColumnFloat64>;
extern template class ColumnUnique<ColumnString>;
extern template class ColumnUnique<ColumnFixedString>;
extern template class ColumnUnique<ColumnDateTime64>;
extern template class ColumnUnique<ColumnTime64>;
extern template class ColumnUnique<ColumnIPv4>;
extern template class ColumnUnique<ColumnIPv6>;
extern template class ColumnUnique<ColumnUUID>;
extern template class ColumnUnique<ColumnDecimal<Decimal32>>;
extern template class ColumnUnique<ColumnDecimal<Decimal64>>;
extern template class ColumnUnique<ColumnDecimal<Decimal128>>;
extern template class ColumnUnique<ColumnDecimal<Decimal256>>;

}
