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
    ColumnWithDictionary(const ColumnWithDictionary & other) = default;

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

    ColumnPtr convertToFullColumn() const { return getDictionary().getNestedColumn()->index(getIndexes(), 0); }
    ColumnPtr convertToFullColumnIfWithDictionary() const override { return convertToFullColumn(); }

    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override { return getIndexes().size(); }

    Field operator[](size_t n) const override { return getDictionary()[getIndexes().getUInt(n)]; }
    void get(size_t n, Field & res) const override { getDictionary().get(getIndexes().getUInt(n), res); }

    StringRef getDataAt(size_t n) const override { return getDictionary().getDataAt(getIndexes().getUInt(n)); }
    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        return getDictionary().getDataAtWithTerminatingZero(getIndexes().getUInt(n));
    }

    UInt64 get64(size_t n) const override { return getDictionary().get64(getIndexes().getUInt(n)); }
    UInt64 getUInt(size_t n) const override { return getDictionary().getUInt(getIndexes().getUInt(n)); }
    Int64 getInt(size_t n) const override { return getDictionary().getInt(getIndexes().getUInt(n)); }
    bool isNullAt(size_t n) const override { return getDictionary().isNullAt(getIndexes().getUInt(n)); }
    ColumnPtr cut(size_t start, size_t length) const override
    {
        return ColumnWithDictionary::create(dictionary.getColumnUniquePtr(), getIndexes().cut(start, length));
    }

    void insert(const Field & x) override;
    void insertDefault() override;

    void insertFrom(const IColumn & src, size_t n) override;
    void insertFromFullColumn(const IColumn & src, size_t n);

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length);
    void insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & positions);

    void insertData(const char * pos, size_t length) override;
    void insertDataWithTerminatingZero(const char * pos, size_t length) override;


    void popBack(size_t n) override { idx.popBack(n); }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        return getDictionary().updateHashWithValue(getIndexes().getUInt(n), hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return ColumnWithDictionary::create(dictionary.getColumnUniquePtr(), getIndexes().filter(filt, result_size_hint));
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return ColumnWithDictionary::create(dictionary.getColumnUniquePtr(), getIndexes().permute(perm, limit));
    }

    ColumnPtr index(const IColumn & indexes_, size_t limit) const override
    {
        return ColumnWithDictionary::create(dictionary.getColumnUniquePtr(), getIndexes().index(indexes_, limit));
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    ColumnPtr replicate(const Offsets & offsets) const override
    {
        return ColumnWithDictionary::create(dictionary.getColumnUniquePtr(), getIndexes().replicate(offsets));
    }

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    void getExtremes(Field & min, Field & max) const override
    {
        return getDictionary().index(getIndexes(), 0)->getExtremes(min, max); /// TODO: optimize
    }

    void reserve(size_t n) override { idx.reserve(n); }

    size_t byteSize() const override { return idx.getPositions()->byteSize() + getDictionary().byteSize(); }
    size_t allocatedBytes() const override { return idx.getPositions()->allocatedBytes() + getDictionary().allocatedBytes(); }

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(idx.getPositionsPtr());

        /// Column doesn't own dictionary if it's shared.
        if (!dictionary.isShared())
            callback(dictionary.getColumnUniquePtr());
    }

    bool valuesHaveFixedSize() const override { return getDictionary().valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return getDictionary().isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return getDictionary().sizeOfValueIfFixed(); }
    bool isNumeric() const override { return getDictionary().isNumeric(); }
    bool withDictionary() const override { return true; }

    const IColumnUnique & getDictionary() const { return dictionary.getColumnUnique(); }
    const ColumnPtr & getDictionaryPtr() const { return dictionary.getColumnUniquePtr(); }
    /// IColumnUnique & getUnique() { return static_cast<IColumnUnique &>(*column_unique->assumeMutable()); }
    /// ColumnPtr getUniquePtr() const { return column_unique; }

    /// IColumn & getIndexes() { return idx.getPositions()->assumeMutableRef(); }
    const IColumn & getIndexes() const { return *idx.getPositions(); }
    const ColumnPtr & getIndexesPtr() const { return idx.getPositions(); }
    size_t getSizeOfIndexType() const { return idx.getSizeOfIndexType(); }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        const IColumn * indexes = &getIndexes();

        switch (idx.getSizeOfIndexType())
        {
            case sizeof(UInt8): return static_cast<const ColumnUInt8 *>(indexes)->getElement(row);
            case sizeof(UInt16): return static_cast<const ColumnUInt16 *>(indexes)->getElement(row);
            case sizeof(UInt32): return static_cast<const ColumnUInt32 *>(indexes)->getElement(row);
            case sizeof(UInt64): return static_cast<const ColumnUInt64 *>(indexes)->getElement(row);
            default: throw Exception("Unexpected size of index type for low cardinality column.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    ///void setIndexes(MutableColumnPtr && indexes_) { indexes = std::move(indexes_); }

    /// Set shared ColumnUnique for empty column with dictionary.
    void setSharedDictionary(const ColumnPtr & column_unique);
    bool isSharedDictionary() const { return dictionary.isShared(); }

    /// Create column new dictionary with only keys that are mentioned in index.
    MutablePtr compact();

    /// Cut + compact.
    MutablePtr cutAndCompact(size_t start, size_t length) const;

    struct DictionaryEncodedColumn
    {
        ColumnPtr dictionary;
        ColumnPtr indexes;
    };

    DictionaryEncodedColumn getMinimalDictionaryEncodedColumn(size_t offset, size_t limit) const;

    ColumnPtr countKeys() const;

    class Index
    {
    public:
        Index();
        Index(const Index & other) = default;
        explicit Index(MutableColumnPtr && positions);
        explicit Index(ColumnPtr positions);

        const ColumnPtr & getPositions() const { return positions; }
        ColumnPtr & getPositionsPtr() { return positions; }
        size_t getPositionAt(size_t row) const;
        void insertPosition(UInt64 position);
        void insertPositionsRange(const IColumn & column, size_t offset, size_t limit);

        void popBack(size_t n) { positions->assumeMutableRef().popBack(n); }
        void reserve(size_t n) { positions->assumeMutableRef().reserve(n); }

        UInt64 getMaxPositionForCurrentType() const;

        static size_t getSizeOfIndexType(const IColumn & column, size_t hint);
        size_t getSizeOfIndexType() const { return size_of_type; }

        void check(size_t max_dictionary_size);
        void checkSizeOfType();

        ColumnPtr detachPositions() { return std::move(positions); }
        void attachPositions(ColumnPtr positions_);

        void countKeys(ColumnUInt64::Container & counts) const;

    private:
        ColumnPtr positions;
        size_t size_of_type = 0;

        void updateSizeOfType() { size_of_type = getSizeOfIndexType(*positions, size_of_type); }
        void expandType();

        template <typename IndexType>
        typename ColumnVector<IndexType>::Container & getPositionsData();

        template <typename IndexType>
        const typename ColumnVector<IndexType>::Container & getPositionsData() const;

        template <typename IndexType>
        void convertPositions();

        template <typename Callback>
        static void callForType(Callback && callback, size_t size_of_type);
    };

private:
    class Dictionary
    {
    public:
        Dictionary(const Dictionary & other) = default;
        explicit Dictionary(MutableColumnPtr && column_unique);
        explicit Dictionary(ColumnPtr column_unique);

        const ColumnPtr & getColumnUniquePtr() const { return column_unique; }
        ColumnPtr & getColumnUniquePtr() { return column_unique; }

        const IColumnUnique & getColumnUnique() const { return static_cast<const IColumnUnique &>(*column_unique); }
        IColumnUnique & getColumnUnique() { return static_cast<IColumnUnique &>(column_unique->assumeMutableRef()); }

        /// Dictionary may be shared for several mutable columns.
        /// Immutable columns may have the same column unique, which isn't necessarily shared dictionary.
        void setShared(const ColumnPtr & dictionary);
        bool isShared() const { return shared; }

        /// Create new dictionary with only keys that are mentioned in positions.
        void compact(ColumnPtr & positions);

    private:
        ColumnPtr column_unique;
        bool shared = false;

        void checkColumn(const IColumn & column);
    };

    Dictionary dictionary;
    Index idx;

    void compactInplace();
    void compactIfSharedDictionary();
};



}
