#pragma once
#include <Columns/IColumn.h>
#include <Columns/IColumnUnique.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

class ColumnWithDictionary final : public COWPtrHelper<IColumn, ColumnWithDictionary>
{
    friend class COWPtrHelper<IColumn, ColumnWithDictionary>;

    ColumnWithDictionary(MutableColumnPtr && column_unique, MutableColumnPtr && indexes);
    ColumnWithDictionary(const ColumnWithDictionary & other);

public:
    std::string getName() const override { return "ColumnWithDictionary"; }
    const char * getFamilyName() const override { return "ColumnWithDictionary"; }


    MutableColumnPtr cloneResized(size_t size) const override
    {
        auto unique_ptr = column_unique;
        return ColumnWithDictionary::create(std::move(unique_ptr)->mutate(), indexes->cloneResized(size));
    }

    size_t size() const override { return indexes->size(); }


    Field operator[](size_t n) const override { return (*column_unique)[indexes->getUInt(n)]; }
    void get(size_t n, Field & res) const override { column_unique->get(indexes->getUInt(n), res); }

    StringRef getDataAt(size_t n) const override { return column_unique->getDataAt(indexes->getUInt(n)); }

    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        return column_unique->getDataAtWithTerminatingZero(indexes->getUInt(n));
    }

    UInt64 get64(size_t n) const override { return column_unique->get64(indexes->getUInt(n)); }

    UInt64 getUInt(size_t n) const override { return column_unique->getUInt(indexes->getUInt(n)); }
    Int64 getInt(size_t n) const override { return column_unique->getInt(indexes->getUInt(n)); }
    bool isNullAt(size_t n) const override { return column_unique->isNullAt(indexes->getUInt(n)); }
    MutableColumnPtr cut(size_t start, size_t length) const override
    {
        auto unique_ptr = column_unique;
        return ColumnWithDictionary::create(std::move(unique_ptr)->mutate(), indexes->cut(start, length));
    }

    void insert(const Field & x) override { getIndexes()->insert(Field(UInt64(getUnique()->uniqueInsert(x)))); }
    void insertFrom(const IColumn & src, size_t n) override { getIndexes()->insert(getUnique()->uniqueInsertFrom(src, n)); }
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
    {
        auto inserted_indexes = getUnique()->uniqueInsertRangeFrom(src, start, length);
        getIndexes()->insertRangeFrom(*inserted_indexes, 0, length);
    }

    void insertData(const char * pos, size_t length) override
    {
        getIndexes()->insert(Field(UInt64(getUnique()->uniqueInsertData(pos, length))));
    }

    void insertDataWithTerminatingZero(const char * pos, size_t length) override
    {
        getIndexes()->insert(Field(UInt64(getUnique()->uniqueInsertDataWithTerminatingZero(pos, length))));
    }

    void insertDefault() override
    {
        getIndexes()->insert(getUnique()->getDefaultValueIndex());
    }

    void popBack(size_t n) override { getIndexes()->popBack(n); }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        return getUnique()->serializeValueIntoArena(indexes->getUInt(n), arena, begin);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        const char * new_pos;
        getIndexes()->insert(getUnique()->uniqueDeserializeAndInsertFromArena(pos, new_pos));
        return new_pos;
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        return getUnique()->updateHashWithValue(indexes->getUInt(n), hash);
    }

    MutableColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        auto unique_ptr = column_unique;
        return ColumnWithDictionary::create(std::move(unique_ptr)->mutate(), indexes->filter(filt, result_size_hint));
    }

    MutableColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        auto unique_ptr = column_unique;
        return ColumnWithDictionary::create(std::move(unique_ptr)->mutate(), indexes->permute(perm, limit));
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override
    {
        const auto & column_with_dictionary = static_cast<const ColumnWithDictionary &>(rhs);
        size_t n_index = indexes->getUInt(n);
        size_t m_index = column_with_dictionary.indexes->getUInt(m);
        return getUnique()->compareAt(n_index, m_index, *column_with_dictionary.column_unique, nan_direction_hint);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override
    {
        size_t unique_limit = std::min(limit, getUnique()->size());
        Permutation unique_perm;
        getUnique()->getPermutation(reverse, unique_limit, nan_direction_hint, unique_perm);

        /// TODO: optimize with sse.

        /// Get indexes per row in column_unique.
        std::vector<std::vector<size_t>> indexes_per_row(getUnique()->size());
        size_t indexes_size = indexes->size();
        for (size_t row = 0; row < indexes_size; ++row)
            indexes_per_row[indexes->getUInt(row)].push_back(row);

        /// Replicate permutation.
        size_t perm_size = std::min(indexes_size, limit);
        res.resize(perm_size);
        size_t perm_index = 0;
        for (size_t row = 0; row < indexes_size && perm_index < perm_size; ++row)
        {
            const auto & row_indexes = indexes_per_row[unique_perm[row]];
            for (auto row_index : row_indexes)
            {
                res[perm_index] = row_index;
                ++perm_index;

                if (perm_index == perm_size)
                    break;
            }
        }
    }

    MutableColumnPtr replicate(const Offsets & offsets) const override
    {
        auto unique_ptr = column_unique;
        return ColumnWithDictionary::create(std::move(unique_ptr)->mutate(), indexes->replicate(offsets));
    }

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        auto columns = indexes->scatter(num_columns, selector);
        for (auto & column : columns)
        {
            auto unique_ptr = column_unique;
            column = ColumnWithDictionary::create(std::move(unique_ptr)->mutate(), std::move(column));
        }

        return columns;
    }

    void gather(ColumnGathererStream & gatherer_stream) override ;
    void getExtremes(Field & min, Field & max) const override { return column_unique->getExtremes(min, max); }

    void reserve(size_t n) override { getIndexes()->reserve(n); }

    size_t byteSize() const override { return indexes->byteSize() + column_unique->byteSize(); }
    size_t allocatedBytes() const override { return indexes->allocatedBytes() + column_unique->allocatedBytes(); }

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(column_unique);
        callback(indexes);
    }

    bool valuesHaveFixedSize() const override { return column_unique->valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return column_unique->isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return column_unique->sizeOfValueIfFixed(); }
    bool isNumeric() const override { return column_unique->isNumeric(); }

    IColumnUnique * getUnique() { return static_cast<IColumnUnique *>(column_unique->assumeMutable().get()); }
    const IColumnUnique * getUnique() const { return static_cast<const IColumnUnique *>(column_unique->assumeMutable().get()); }
    const ColumnPtr & getUniquePtr() const { return column_unique; }

    IColumn * getIndexes() { return indexes->assumeMutable().get(); }
    const IColumn * getIndexes() const { return indexes.get(); }
    const ColumnPtr & getIndexesPtr() const { return indexes; }

private:
    ColumnPtr column_unique;
    ColumnPtr indexes;

};



}
