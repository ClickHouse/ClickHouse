#include <Columns/IColumnUnique.h>
#include <Common/HashTable/HashMap.h>
#include <ext/range.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include "ColumnString.h"

class NullMap;
namespace DB
{

template <typename ColumnType, typename IndexType>
class ColumnUnique final : public COWPtrHelper<IColumnUnique, ColumnUnique>
{
    friend class COWPtrHelper<IColumnUnique, ColumnUnique>;

private:
    explicit ColumnUnique(const ColumnPtr & holder);
    explicit ColumnUnique(bool is_nullable) : column_holder(ColumnType::create(numSpecialValues())), is_nullable(is_nullable) {}
    ColumnUnique(const ColumnUnique & other) : column_holder(other.column_holder), is_nullable(other.is_nullable) {}

public:
    ColumnPtr getNestedColumn() const override { return column_holder; }
    size_t uniqueInsert(const Field & x) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    ColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    size_t uniqueInsertData(const char * pos, size_t length) override;
    size_t uniqueInsertDataWithTerminatingZero(const char * pos, size_t length) override;
    size_t uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) override;

    size_t getDefaultValueIndex() const override { return is_nullable ? 1 : 0; }
    size_t getNullValueIndex() const override;
    bool canContainNulls() const override { return is_nullable; }

    Field operator[](size_t n) const override { return (*column_holder)[n]; }
    void get(size_t n, Field & res) const override { column_holder->get(n, res); }
    StringRef getDataAt(size_t n) const override { return column_holder->getDataAt(n); }
    StringRef getDataAtWithTerminatingZero(size_t n) const override { return column_holder->getDataAtWithTerminatingZero(n); }
    UInt64 get64(size_t n) const override { return column_holder->get64(n); }
    UInt64 getUInt(size_t n) const override { return column_holder->getUInt(n); }
    Int64 getInt(size_t n) const override { return column_holder->getInt(n); }
    bool isNullAt(size_t n) const override { return column_holder->isNullAt(n); }
    MutableColumnPtr cut(size_t start, size_t length) const override { return column_holder->cut(start, length); }
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override { return column_holder->serializeValueIntoArena(n, arena, begin); }
    const char * deserializeAndInsertFromArena(const char * pos) override { return column_holder->deserializeAndInsertFromArena(pos); }
    void updateHashWithValue(size_t n, SipHash & hash) const override { return column_holder->updateHashWithValue(n, hash); }
    MutableColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override { return column_holder->filter(filt, result_size_hint); }
    MutableColumnPtr permute(const Permutation & perm, size_t limit) const override { return column_holder->permute(perm, limit); }
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override { column_holder->compareAt(n, m, rhs, nan_direction_hint); }
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override { column_holder->getPermutation(reverse, limit, nan_direction_hint, res); }
    MutableColumnPtr replicate(const Offsets & offsets) const override { return column_holder->replicate(offsets); }
    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override { return column_holder->scatter(num_columns, selector); }
    void getExtremes(Field & min, Field & max) const override { column_holder->getExtremes(min, max); }
    bool valuesHaveFixedSize() const override { return column_holder->valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return column_holder->isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return column_holder->sizeOfValueIfFixed(); }
    bool isNumeric() const override { return column_holder->isNumeric(); }

    size_t byteSize() const override { return column_holder->byteSize(); }
    size_t allocatedBytes() const override { return column_holder->allocatedBytes() + (index ? index->getBufferSizeInBytes() : 0); }
    void forEachSubcolumn(ColumnCallback callback) override { callback(column_holder); }

private:

    struct StringRefWrapper
    {
        const ColumnType * column = nullptr;
        size_t row = 0;

        StringRef ref;

        StringRefWrapper(const ColumnType * column, size_t row) : column(column), row(row) {}
        StringRefWrapper(StringRef ref) : ref(ref) {}

        operator StringRef() const { return column ? column->getDataAt(row) : ref; }

        bool operator==(const StringRefWrapper & other)
        {
            return (column && column == other.column && row == other.row) || StringRef(*this) == other;
        }
    };

    using IndexMapType = HashMap<StringRefWrapper, IndexType, StringRefHash>;

    ColumnPtr column_holder;
    /// Lazy initialized.
    std::unique_ptr<IndexMapType> index;

    bool is_nullable;

    size_t numSpecialValues() const { return is_nullable ? 2 : 1; }

    void buildIndex();
    ColumnType * getRawColumnPtr() const { return static_cast<ColumnType *>(column_holder.get()); }
    IndexType insert(const StringRefWrapper & ref, IndexType value);

};

template <typename ColumnType, typename IndexType>
ColumnUnique::ColumnUnique(const ColumnPtr & holder) : column_holder(holder)
{
    if (column_holder->isColumnNullable())
    {
        auto column_nullable = static_cast<const ColumnNullable *>(column_holder.get());
        column_holder = column_nullable->getNestedColumnPtr();
        is_nullable = true;
    }
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique::getNullValueIndex() const override
{
    if (!is_nullable)
        throw Exception("ColumnUnique can't contain null values.");

    return 0;
}

template <typename ColumnType, typename IndexType>
void ColumnUnique::buildIndex()
{
    if (index)
        return;

    auto column = getRawColumnPtr();
    index = std::make_unique<IndexMapType>();

    for (auto row : ext::range(numSpecialValues(), column->size()))
    {
        (*index)[StringRefWrapper(column, row)] = row;
    }
}

template <typename ColumnType, typename IndexType>
IndexType ColumnUnique::insert(const StringRefWrapper & ref, IndexType value)
{
    if (!index)
        buildIndex();

    IndexType::iterator it;
    bool inserted;
    index->emplace(ref, it, inserted);

    if (inserted)
        it->second = value;

    return it->second;
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique::uniqueInsert(const Field & x) override
{
    if (x.getType() == Field::Types::Null)
        return getNullValueIndex();

    auto column = getRawColumnPtr();
    IndexType prev_size = static_cast<IndexType>(column->size());

    if ((*column)[getDefaultValueIndex()] == x)
        return getDefaultValueIndex();

    column->insert(x);
    auto pos = insert(StringRefWrapper(column, prev_size), prev_size);
    if (pos != prev_size)
        column->popBack(1);

    return static_cast<size_t>(pos);
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique::uniqueInsertFrom(const IColumn & src, size_t n) override
{
    auto ref = src.getDataAt(n);
    return uniqueInsertData(ref.data, ref.size);
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique::uniqueInsertData(const char * data, size_t length) override
{
    auto column = getRawColumnPtr();

    if (column->getDataAt(getDefaultValueIndex()) == StringRef(data, length))
        return getDefaultValueIndex();

    IndexType size = static_cast<IndexType>(column->size());

    if (!index->has(StringRefWrapper(StringRef(data, length))))
    {
        column->insertData(data, length);
        return static_cast<size_t>(insert(StringRefWrapper(StringRef(data, length)), size));
    }

    return size;
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique::uniqueInsertDataWithTerminatingZero(const char * data, size_t length) override
{
    if (std::is_same<ColumnType, ColumnString>::value)
        return uniqueInsertData(data, length - 1);

    if (column_holder->valuesHaveFixedSize())
        return uniqueInsertData(data, length);

    /// Don't know if data actually has terminating zero. So, insert it firstly.

    auto column = getRawColumnPtr();
    size_t prev_size = column->size();
    column->insertDataWithTerminatingZero(data, length);

    if (column->compareAt(getDefaultValueIndex(), prev_size, *column, 1) == 0)
    {
        column->popBack(1);
        return getDefaultValueIndex();
    }

    auto pos = insert(StringRefWrapper(column, prev_size), prev_size);
    if (pos != prev_size)
        column->popBack(1);

    return static_cast<size_t>(pos);
}

template <typename ColumnType, typename IndexType>
size_t ColumnUnique::uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) override
{
    auto column = getRawColumnPtr();
    size_t prev_size = column->size();
    new_pos = column->deserializeAndInsertFromArena(pos);

    if (column->compareAt(getDefaultValueIndex(), prev_size, *column, 1) == 0)
    {
        column->popBack(1);
        return getDefaultValueIndex();
    }

    auto index_pos = insert(StringRefWrapper(StringRef(column, prev_size)), prev_size);
    if (index_pos != prev_size)
        column->popBack(1);

    return static_cast<size_t>(index_pos);
}

template <typename ColumnType, typename IndexType>
ColumnPtr ColumnUnique::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override
{
    if (!index)
        buildIndex();

    const ColumnType * src_column;
    const NullMap * null_map = nullptr;

    if (src_column->isNullable())
    {
        auto nullable_column = static_cast<const ColumnNullable *>(&src);
        src_column = static_cast<const ColumnType *>(&nullable_column->getNestedColumn());
        null_map = &nullable_column->getNullMapData();
    }
    else
        src_column = static_cast<const ColumnType *>(&src);

    auto column = getRawColumnPtr();
    IColumn::Filter filter(src_column->size(), 0);
    auto positions_column = ColumnVector<IndexType>::create(length);
    auto & positions = positions_column.getData();

    size_t next_position = column->size();
    for (auto i : ext::range(0, length))
    {
        auto row = start + i;

        if (column->compareAt(getDefaultValueIndex(), row, *src_column, 1) == 0)
            positions[i] = getDefaultValueIndex();
        else if (null_map && (*null_map)[row])
            positions[i] = getNullValueIndex();
        else
        {
            auto it = index->find(StringRefWrapper(&src_column, row));
            if (it == index->end())
            {
                filter[row] = 1;
                positions[i] = next_position;
                ++next_position;
            }
            else
                positions[i] = it->second;
        }
    }

    auto filtered_column_ptr = src_column->filter(filter);
    auto filtered_column = static_cast<ColumnType &>(*filtered_column_ptr);

    size_t filtered_size = filtered_column->size();

    size_t prev_size = column->size();
    column->insertRangeFrom(filtered_column, 0, filtered_size);

    if (filtered_size)
    {
        for (auto row : ext::range(prev_size, prev_size + filtered_size))
            (*index)[StringRefWrapper(column, row)] = row;
    }

    return positions_column;
}

}
