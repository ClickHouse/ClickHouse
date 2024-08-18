#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/IColumnUnique.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * How data is stored (in a nutshell):
 * we have a dictionary @e reverse_index in ColumnUnique that holds pairs (DataType, UIntXX) and a column
 * with UIntXX holding actual data indices.
 * To obtain the value's index, call #getOrFindIndex.
 * To operate on the data (so called indices column), call #getIndexes.
 *
 * @note The indices column always contains the default value (empty StringRef) with the first index.
 */
class ColumnLowCardinality final : public COWHelper<IColumnHelper<ColumnLowCardinality>, ColumnLowCardinality>
{
    friend class COWHelper<IColumnHelper<ColumnLowCardinality>, ColumnLowCardinality>;

    ColumnLowCardinality(MutableColumnPtr && column_unique, MutableColumnPtr && indexes, bool is_shared = false);
    ColumnLowCardinality(const ColumnLowCardinality & other) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnLowCardinality>, ColumnLowCardinality>;
    static Ptr create(const ColumnPtr & column_unique_, const ColumnPtr & indexes_, bool is_shared = false)
    {
        return ColumnLowCardinality::create(column_unique_->assumeMutable(), indexes_->assumeMutable(), is_shared);
    }

    static MutablePtr create(MutableColumnPtr && column_unique, MutableColumnPtr && indexes, bool is_shared = false)
    {
        return Base::create(std::move(column_unique), std::move(indexes), is_shared);
    }

    std::string getName() const override { return "LowCardinality(" + getDictionary().getNestedColumn()->getName() + ")"; }
    const char * getFamilyName() const override { return "LowCardinality"; }
    TypeIndex getDataType() const override { return TypeIndex::LowCardinality; }

    ColumnPtr convertToFullColumn() const { return getDictionary().getNestedColumn()->index(getIndexes(), 0); }
    ColumnPtr convertToFullColumnIfLowCardinality() const override { return convertToFullColumn(); }

    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override { return getIndexes().size(); }

    Field operator[](size_t n) const override { return getDictionary()[getIndexes().getUInt(n)]; }
    void get(size_t n, Field & res) const override { getDictionary().get(getIndexes().getUInt(n), res); }

    StringRef getDataAt(size_t n) const override { return getDictionary().getDataAt(getIndexes().getUInt(n)); }

    bool isDefaultAt(size_t n) const override { return getDictionary().isDefaultAt(getIndexes().getUInt(n)); }
    UInt64 get64(size_t n) const override { return getDictionary().get64(getIndexes().getUInt(n)); }
    UInt64 getUInt(size_t n) const override { return getDictionary().getUInt(getIndexes().getUInt(n)); }
    Int64 getInt(size_t n) const override { return getDictionary().getInt(getIndexes().getUInt(n)); }
    Float64 getFloat64(size_t n) const override { return getDictionary().getFloat64(getIndexes().getUInt(n)); }
    Float32 getFloat32(size_t n) const override { return getDictionary().getFloat32(getIndexes().getUInt(n)); }
    bool getBool(size_t n) const override { return getDictionary().getBool(getIndexes().getUInt(n)); }
    bool isNullAt(size_t n) const override { return getDictionary().isNullAt(getIndexes().getUInt(n)); }
    ColumnPtr cut(size_t start, size_t length) const override
    {
        return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().cut(start, length));
    }

    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    void insertDefault() override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
#endif
    void insertFromFullColumn(const IColumn & src, size_t n);

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    void insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length);
    void insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & positions);

    void insertData(const char * pos, size_t length) override;

    void popBack(size_t n) override { idx.popBack(n); }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;

    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        getDictionary().updateHashWithValue(getIndexes().getUInt(n), hash);
    }

    WeakHash32 getWeakHash32() const override;

    void updateHashFast(SipHash &) const override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().filter(filt, result_size_hint));
    }

    void expand(const Filter & mask, bool inverted) override
    {
        idx.getPositionsPtr()->expand(mask, inverted);
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().permute(perm, limit));
    }

    ColumnPtr index(const IColumn & indexes_, size_t limit) const override
    {
        return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().index(indexes_, limit));
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator &) const override;

    bool hasEqualValues() const override;

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;

    void getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges& equal_ranges) const override;

    size_t estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const override;

    ColumnPtr replicate(const Offsets & offsets) const override
    {
        return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().replicate(offsets));
    }

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void getExtremes(Field & min, Field & max) const override
    {
        dictionary.getColumnUnique().getNestedColumn()->index(getIndexes(), 0)->getExtremes(min, max); /// TODO: optimize
    }

    void reserve(size_t n) override { idx.reserve(n); }
    size_t capacity() const override { return idx.capacity(); }
    void shrinkToFit() override { idx.shrinkToFit(); }

    /// Don't count the dictionary size as it can be shared between different blocks.
    size_t byteSize() const override { return idx.getPositions()->byteSize(); }

    size_t byteSizeAt(size_t n) const override { return getDictionary().byteSizeAt(getIndexes().getUInt(n)); }
    size_t allocatedBytes() const override { return idx.getPositions()->allocatedBytes() + getDictionary().allocatedBytes(); }

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        callback(idx.getPositionsPtr());

        /// Column doesn't own dictionary if it's shared.
        if (!dictionary.isShared())
            callback(dictionary.getColumnUniquePtr());
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*idx.getPositionsPtr());
        idx.getPositionsPtr()->forEachSubcolumnRecursively(callback);

        /// Column doesn't own dictionary if it's shared.
        if (!dictionary.isShared())
        {
            callback(*dictionary.getColumnUniquePtr());
            dictionary.getColumnUniquePtr()->forEachSubcolumnRecursively(callback);
        }
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&rhs))
            return idx.getPositions()->structureEquals(*rhs_low_cardinality->idx.getPositions())
                && dictionary.getColumnUnique().structureEquals(rhs_low_cardinality->dictionary.getColumnUnique());
        return false;
    }

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return getIndexes().getRatioOfDefaultRows(sample_ratio);
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return getIndexes().getNumberOfDefaultRows();
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        getIndexes().getIndicesOfNonDefaultRows(indices, from, limit);
    }

    bool valuesHaveFixedSize() const override { return getDictionary().valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return false; }
    size_t sizeOfValueIfFixed() const override { return getDictionary().sizeOfValueIfFixed(); }
    bool isNumeric() const override { return getDictionary().isNumeric(); }
    bool lowCardinality() const override { return true; }
    bool isCollationSupported() const override { return getDictionary().getNestedColumn()->isCollationSupported(); }

    /**
     * Checks if the dictionary column is Nullable(T).
     * So LC(Nullable(T)) would return true, LC(U) -- false.
     */
    bool nestedIsNullable() const { return isColumnNullable(*dictionary.getColumnUnique().getNestedColumn()); }
    bool nestedCanBeInsideNullable() const { return dictionary.getColumnUnique().getNestedColumn()->canBeInsideNullable(); }
    void nestedToNullable() { dictionary.getColumnUnique().nestedToNullable(); }
    void nestedRemoveNullable() { dictionary.getColumnUnique().nestedRemoveNullable(); }
    MutableColumnPtr cloneNullable() const;

    ColumnPtr cloneWithDefaultOnNull() const;

    const IColumnUnique & getDictionary() const { return dictionary.getColumnUnique(); }
    IColumnUnique & getDictionary() { return dictionary.getColumnUnique(); }
    const ColumnPtr & getDictionaryPtr() const { return dictionary.getColumnUniquePtr(); }
    ColumnPtr & getDictionaryPtr() { return dictionary.getColumnUniquePtr(); }
    /// IColumnUnique & getUnique() { return static_cast<IColumnUnique &>(*column_unique); }
    /// ColumnPtr getUniquePtr() const { return column_unique; }

    /// IColumn & getIndexes() { return *idx.getPositions(); }
    const IColumn & getIndexes() const { return *idx.getPositions(); }
    const ColumnPtr & getIndexesPtr() const { return idx.getPositions(); }
    size_t getSizeOfIndexType() const { return idx.getSizeOfIndexType(); }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        const IColumn * indexes = &getIndexes();

        switch (idx.getSizeOfIndexType())
        {
            case sizeof(UInt8): return assert_cast<const ColumnUInt8 *>(indexes)->getElement(row);
            case sizeof(UInt16): return assert_cast<const ColumnUInt16 *>(indexes)->getElement(row);
            case sizeof(UInt32): return assert_cast<const ColumnUInt32 *>(indexes)->getElement(row);
            case sizeof(UInt64): return assert_cast<const ColumnUInt64 *>(indexes)->getElement(row);
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
        }
    }

    ///void setIndexes(MutableColumnPtr && indexes_) { indexes = std::move(indexes_); }

    /// Set shared ColumnUnique for empty low cardinality column.
    void setSharedDictionary(const ColumnPtr & column_unique);
    bool isSharedDictionary() const { return dictionary.isShared(); }

    /// Create column with new dictionary from column part.
    /// Dictionary will have only keys that are mentioned in index.
    MutablePtr cutAndCompact(size_t start, size_t length) const;

    struct DictionaryEncodedColumn
    {
        ColumnPtr dictionary;
        ColumnPtr indexes;
    };

    DictionaryEncodedColumn getMinimalDictionaryEncodedColumn(UInt64 offset, UInt64 limit) const;

    ColumnPtr countKeys() const;

    bool containsNull() const;

    class Index
    {
    public:
        Index();
        Index(const Index & other) = default;
        explicit Index(MutableColumnPtr && positions_);
        explicit Index(ColumnPtr positions_);

        const ColumnPtr & getPositions() const { return positions; }
        WrappedPtr & getPositionsPtr() { return positions; }
        const WrappedPtr & getPositionsPtr() const { return positions; }
        size_t getPositionAt(size_t row) const;
        void insertPosition(UInt64 position);
        void insertPositionsRange(const IColumn & column, UInt64 offset, UInt64 limit);

        void popBack(size_t n) { positions->popBack(n); }
        void reserve(size_t n) { positions->reserve(n); }
        size_t capacity() const { return positions->capacity(); }
        void shrinkToFit() { positions->shrinkToFit(); }

        UInt64 getMaxPositionForCurrentType() const;

        static size_t getSizeOfIndexType(const IColumn & column, size_t hint);
        size_t getSizeOfIndexType() const { return size_of_type; }

        void checkSizeOfType();

        MutableColumnPtr detachPositions() { return IColumn::mutate(std::move(positions)); }
        void attachPositions(MutableColumnPtr positions_);

        void countKeys(ColumnUInt64::Container & counts) const;

        bool containsDefault() const;

        WeakHash32 getWeakHash(const WeakHash32 & dict_hash) const;

        void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const PaddedPODArray<UInt64> & dict_sizes) const;

    private:
        WrappedPtr positions;
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
        explicit Dictionary(MutableColumnPtr && column_unique, bool is_shared);
        explicit Dictionary(ColumnPtr column_unique, bool is_shared);

        const WrappedPtr & getColumnUniquePtr() const { return column_unique; }
        WrappedPtr & getColumnUniquePtr() { return column_unique; }

        const IColumnUnique & getColumnUnique() const { return static_cast<const IColumnUnique &>(*column_unique); }
        IColumnUnique & getColumnUnique() { return static_cast<IColumnUnique &>(*column_unique); }

        /// Dictionary may be shared for several mutable columns.
        /// Immutable columns may have the same column unique, which isn't necessarily shared dictionary.
        void setShared(const ColumnPtr & column_unique_);
        bool isShared() const { return shared; }

        /// Create new dictionary with only keys that are mentioned in positions.
        void compact(MutableColumnPtr & positions);

        static MutableColumnPtr compact(const IColumnUnique & column_unique, MutableColumnPtr & positions);

    private:
        WrappedPtr column_unique;
        bool shared = false;
    };

    Dictionary dictionary;
    Index idx;

    void compactInplace();
    void compactIfSharedDictionary();

    int compareAtImpl(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator * collator=nullptr) const;

    void getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator = nullptr) const;
};

bool isColumnLowCardinalityNullable(const IColumn & column);


}
