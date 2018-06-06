#pragma once
#include <Columns/IColumnUnique.h>
#include <Common/HashTable/HashMap.h>
#include <ext/range.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>

class NullMap;


template <typename ColumnType>
struct StringRefWrapper
{
    const ColumnType * column = nullptr;
    size_t row = 0;

    StringRef ref;

    StringRefWrapper(const ColumnType * column, size_t row) : column(column), row(row) {}
    StringRefWrapper(StringRef ref) : ref(ref) {}
    StringRefWrapper(const StringRefWrapper & other) = default;
    StringRefWrapper & operator =(int) { column = nullptr; ref.data = nullptr; return *this; }
    bool operator ==(int) const { return nullptr == column && nullptr == ref.data; }
    StringRefWrapper() {}

    operator StringRef() const { return column ? column->getDataAt(row) : ref; }

    bool operator==(const StringRefWrapper<ColumnType> & other) const
    {
        return (column && column == other.column && row == other.row) || StringRef(*this) == other;
    }

};

namespace ZeroTraits
{
    template <typename ColumnType>
    bool check(const StringRefWrapper<ColumnType> x) { return nullptr == x.column; }

    template <typename ColumnType>
    void set(StringRefWrapper<ColumnType> & x) { x.column = nullptr; }
};


namespace DB
{

template <typename ColumnType, typename IndexType>
class ColumnUnique final : public COWPtrHelper<IColumnUnique, ColumnUnique<ColumnType, IndexType>>
{
    friend class COWPtrHelper<IColumnUnique, ColumnUnique<ColumnType, IndexType>>;

private:
    explicit ColumnUnique(MutableColumnPtr && holder);
    explicit ColumnUnique(const DataTypePtr & type);
    ColumnUnique(const ColumnUnique & other)
            : column_holder(other.column_holder), nullable_column(other.nullable_column)
            , nullable_column_map(other.nullable_column_map), is_nullable(other.is_nullable) {}

public:
    const ColumnPtr & getNestedColumn() const override;
    size_t uniqueInsert(const Field & x) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    ColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
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
               + (index ? index->getBufferSizeInBytes() : 0)
               + (nullable_column ? nullable_column->allocatedBytes() : 0);
    }
    void forEachSubcolumn(IColumn::ColumnCallback callback) override
    {
        callback(is_nullable ? nullable_column : column_holder);
        /// If column was mutated, we need to restore ptrs.
        if (is_nullable)
        {
            auto & column_nullable = static_cast<ColumnNullable &>(nullable_column->assumeMutableRef());
            column_holder = column_nullable.getNestedColumnPtr();
            nullable_column_map = &column_nullable.getNullMapData();
        }
    }

private:

    using IndexMapType = HashMap<StringRefWrapper<ColumnType>, IndexType, StringRefHash>;

    ColumnPtr column_holder;

    /// For DataTypeNullable, nullptr otherwise.
    ColumnPtr nullable_column;
    NullMap * nullable_column_map = nullptr;

    /// Lazy initialized.
    std::unique_ptr<IndexMapType> index;

    bool is_nullable;

    size_t numSpecialValues() const { return is_nullable ? 2 : 1; }

    void buildIndex();
    ColumnType * getRawColumnPtr() { return static_cast<ColumnType *>(column_holder->assumeMutable().get()); }
    const ColumnType * getRawColumnPtr() const { return static_cast<ColumnType *>(column_holder.get()); }
    IndexType insertIntoMap(const StringRefWrapper<ColumnType> & ref, IndexType value);

};

template <typename ColumnType, typename IndexType>
ColumnUnique<ColumnType, IndexType>::ColumnUnique(const DataTypePtr & type) : is_nullable(type->isNullable())
{
    if (is_nullable)
    {
        nullable_column = type->createColumn()->cloneResized(numSpecialValues());
        auto & column_nullable = static_cast<ColumnNullable &>(nullable_column->assumeMutableRef());
        column_holder = column_nullable.getNestedColumnPtr();
        nullable_column_map = &column_nullable.getNullMapData();
        (*nullable_column_map)[getDefaultValueIndex()] = 0;
    }
    else
        column_holder = type->createColumn()->cloneResized(numSpecialValues());
}

template <typename ColumnType, typename IndexType>
ColumnUnique<ColumnType, IndexType>::ColumnUnique(MutableColumnPtr && holder) : column_holder(std::move(holder))
{
    if (column_holder->isColumnNullable())
    {
        nullable_column = std::move(column_holder);
        auto & column_nullable = static_cast<ColumnNullable &>(nullable_column->assumeMutableRef());
        column_holder = column_nullable.getNestedColumnPtr();
        nullable_column_map = &column_nullable.getNullMapData();
        is_nullable = true;
    }
}

template <typename ColumnType, typename IndexType>
const ColumnPtr& ColumnUnique<ColumnType, IndexType>::getNestedColumn() const
{
    if (is_nullable)
    {
        nullable_column_map->resize_fill(column_holder->size());
        return nullable_column;
    }
    return column_holder;
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique<ColumnType, IndexType>::getNullValueIndex() const
{
    if (!is_nullable)
        throw Exception("ColumnUnique can't contain null values.");

    return 0;
}

template <typename ColumnType, typename IndexType>
void ColumnUnique<ColumnType, IndexType>::buildIndex()
{
    if (index)
        return;

    auto column = getRawColumnPtr();
    index = std::make_unique<IndexMapType>();

    for (auto row : ext::range(numSpecialValues(), column->size()))
    {
        (*index)[StringRefWrapper<ColumnType>(column, row)] = row;
    }
}

template <typename ColumnType, typename IndexType>
IndexType ColumnUnique<ColumnType, IndexType>::insertIntoMap(const StringRefWrapper<ColumnType> & ref, IndexType value)
{
    if (!index)
        buildIndex();

    using IteratorType = typename IndexMapType::iterator;
    IteratorType it;
    bool inserted;
    index->emplace(ref, it, inserted);

    if (inserted)
        it->second = value;

    return it->second;
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique<ColumnType, IndexType>::uniqueInsert(const Field & x)
{
    if (x.getType() == Field::Types::Null)
        return getNullValueIndex();

    auto column = getRawColumnPtr();
    auto prev_size = static_cast<IndexType>(column->size());

    if ((*column)[getDefaultValueIndex()] == x)
        return getDefaultValueIndex();

    column->insert(x);
    auto pos = insertIntoMap(StringRefWrapper<ColumnType>(column, prev_size), prev_size);
    if (pos != prev_size)
        column->popBack(1);

    return pos;
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique<ColumnType, IndexType>::uniqueInsertFrom(const IColumn & src, size_t n)
{
    if (is_nullable && src.isNullAt(n))
        return getNullValueIndex();

    auto ref = src.getDataAt(n);
    return uniqueInsertData(ref.data, ref.size);
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique<ColumnType, IndexType>::uniqueInsertData(const char * pos, size_t length)
{
    if (!index)
        buildIndex();

    auto column = getRawColumnPtr();

    if (column->getDataAt(getDefaultValueIndex()) == StringRef(pos, length))
        return getDefaultValueIndex();

    auto size = static_cast<IndexType>(column->size());
    auto iter = index->find(StringRefWrapper<ColumnType>(StringRef(pos, length)));

    if (iter == index->end())
    {
        column->insertData(pos, length);
        return insertIntoMap(StringRefWrapper<ColumnType>(column, size), size);
    }

    return iter->second;
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique<ColumnType, IndexType>::uniqueInsertDataWithTerminatingZero(const char * pos, size_t length)
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

    auto position = insertIntoMap(StringRefWrapper<ColumnType>(column, prev_size), prev_size);
    if (position != prev_size)
        column->popBack(1);

    return static_cast<size_t>(position);
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique<ColumnType, IndexType>::uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos)
{
    auto column = getRawColumnPtr();
    size_t prev_size = column->size();
    new_pos = column->deserializeAndInsertFromArena(pos);

    if (column->compareAt(getDefaultValueIndex(), prev_size, *column, 1) == 0)
    {
        column->popBack(1);
        return getDefaultValueIndex();
    }

    auto index_pos = insertIntoMap(StringRefWrapper<ColumnType>(column, prev_size), prev_size);
    if (index_pos != prev_size)
        column->popBack(1);

    return static_cast<size_t>(index_pos);
}

template <typename ColumnType, typename IndexType>
ColumnPtr ColumnUnique<ColumnType, IndexType>::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (!index)
        buildIndex();

    const ColumnType * src_column;
    const NullMap * null_map = nullptr;

    if (src.isColumnNullable())
    {
        auto nullable_column = static_cast<const ColumnNullable *>(&src);
        src_column = static_cast<const ColumnType *>(&nullable_column->getNestedColumn());
        null_map = &nullable_column->getNullMapData();
    }
    else
        src_column = static_cast<const ColumnType *>(&src);

    auto column = getRawColumnPtr();
    auto positions_column = ColumnVector<IndexType>::create(length);
    auto & positions = positions_column->getData();

    size_t next_position = column->size();
    for (auto i : ext::range(0, length))
    {
        auto row = start + i;

        if (null_map && (*null_map)[row])
            positions[i] = getNullValueIndex();
        else if (column->compareAt(getDefaultValueIndex(), row, *src_column, 1) == 0)
            positions[i] = getDefaultValueIndex();
        else
        {
            auto it = index->find(StringRefWrapper<ColumnType>(src_column, row));
            if (it == index->end())
            {
                positions[i] = next_position;
                auto ref = src_column->getDataAt(row);
                column->insertData(ref.data, ref.size);
                (*index)[StringRefWrapper<ColumnType>(column, next_position)] = next_position;
                ++next_position;
            }
            else
                positions[i] = it->second;
        }
    }

    return positions_column;
}

}
