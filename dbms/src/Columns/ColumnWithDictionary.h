#pragma once
#include <Columns/IColumn.h>
#include <Columns/IColumnUnique.h>
#include <Common/typeid_cast.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include "ColumnsNumber.h"

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
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWPtrHelper<IColumn, ColumnWithDictionary>;
    static Ptr create(const ColumnPtr & column_unique_, const ColumnPtr & indexes_)
    {
        return ColumnWithDictionary::create(column_unique_->assumeMutable(), indexes_->assumeMutable());
    }

    template <typename ... Args, typename = typename std::enable_if<IsMutableColumns<Args ...>::value>::type>
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }


    std::string getName() const override { return "ColumnWithDictionary"; }
    const char * getFamilyName() const override { return "ColumnWithDictionary"; }

    ColumnPtr convertToFullColumn() const
    {
        return getUnique()->getNestedColumn()->index(indexes, 0);
    }

    ColumnPtr convertToFullColumnIfWithDictionary() const override { return convertToFullColumn(); }

    MutableColumnPtr cloneResized(size_t size) const override
    {
        auto unique_ptr = column_unique;
        return ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), indexes->cloneResized(size));
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
    ColumnPtr cut(size_t start, size_t length) const override
    {
        return ColumnWithDictionary::create(column_unique, indexes->cut(start, length));
    }

    void insert(const Field & x) override { getIndexes()->insert(Field(UInt64(getUnique()->uniqueInsert(x)))); }

    void insertFromFullColumn(const IColumn & src, size_t n)
    {
        getIndexes()->insert(getUnique()->uniqueInsertFrom(src, n));
    }
    void insertFrom(const IColumn & src, size_t n) override
    {
        if (!typeid_cast<const ColumnWithDictionary *>(&src))
            throw Exception("Expected ColumnWithDictionary, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

        auto & src_with_dict = static_cast<const ColumnWithDictionary &>(src);
        size_t idx = src_with_dict.getIndexes()->getUInt(n);
        insertFromFullColumn(*src_with_dict.getUnique()->getNestedColumn(), idx);
    }

    void insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length)
    {
        auto inserted_indexes = getUnique()->uniqueInsertRangeFrom(src, start, length);
        getIndexes()->insertRangeFrom(*inserted_indexes, 0, length);
    }
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
    {
        if (!typeid_cast<const ColumnWithDictionary *>(&src))
            throw Exception("Expected ColumnWithDictionary, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

        auto & src_with_dict = static_cast<const ColumnWithDictionary &>(src);
        auto & src_nested = src_with_dict.getUnique()->getNestedColumn();
        auto inserted_idx = getUnique()->uniqueInsertRangeFrom(*src_nested, 0, src_nested->size());
        auto idx = inserted_idx->index(src_with_dict.getIndexes()->cut(start, length), 0);
        getIndexes()->insertRangeFrom(*idx, 0, length);
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

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return ColumnWithDictionary::create(column_unique, indexes->filter(filt, result_size_hint));
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return ColumnWithDictionary::create(column_unique, indexes->permute(perm, limit));
    }

    ColumnPtr index(const ColumnPtr & indexes_, size_t limit) const override
    {
        return ColumnWithDictionary::create(column_unique, indexes->index(indexes_, limit));
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
        getUnique()->getNestedColumn()->getPermutation(reverse, unique_limit, nan_direction_hint, unique_perm);

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

    ColumnPtr replicate(const Offsets & offsets) const override
    {
        return ColumnWithDictionary::create(column_unique, indexes->replicate(offsets));
    }

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        auto columns = indexes->scatter(num_columns, selector);
        for (auto & column : columns)
        {
            auto unique_ptr = column_unique;
            column = ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), std::move(column));
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

    void setIndexes(MutableColumnPtr && indexes_) { indexes = std::move(indexes_); }

    bool withDictionary() const override { return true; }

private:
    ColumnPtr column_unique;
    ColumnPtr indexes;

};



}
