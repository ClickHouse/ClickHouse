#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnIndex.h>

class Collator;

namespace DB
{


/** Column for replicated representation.
 *  It stores original column and indexes in this column.
 *  Used to perform lazy column replication.
 */
class ColumnReplicated final : public COWHelper<IColumnHelper<ColumnReplicated>, ColumnReplicated>
{
private:
    friend class COWHelper<IColumnHelper<ColumnReplicated>, ColumnReplicated>;

    explicit ColumnReplicated(MutableColumnPtr && nested_column_);
    ColumnReplicated(MutableColumnPtr && nested_column_, MutableColumnPtr && indexes_);
    ColumnReplicated(MutableColumnPtr && nested_column_, ColumnIndex && indexes_);
    ColumnReplicated(const ColumnReplicated &) = default;

public:
    using Base = COWHelper<IColumnHelper<ColumnReplicated>, ColumnReplicated>;

    static Ptr create(const ColumnPtr & nested_column_, const ColumnPtr & indexes_)
    {
        return Base::create(nested_column_->assumeMutable(), indexes_->assumeMutable());
    }

    static Ptr create(const ColumnPtr & nested_column_)
    {
        return Base::create(nested_column_->assumeMutable());
    }

    static MutablePtr create(MutableColumnPtr && nested_column_, MutableColumnPtr && indexes_)
    {
        return Base::create(std::move(nested_column_), std::move(indexes_));
    }

    static MutablePtr create(MutableColumnPtr && nested_column_)
    {
        return Base::create(std::move(nested_column_));
    }

    static MutablePtr create(MutableColumnPtr && nested_column_, ColumnIndex && indexes_)
    {
        return Base::create(std::move(nested_column_), std::move(indexes_));
    }

    static Ptr create(ColumnPtr & nested_column_, ColumnIndex && indexes_)
    {
        return Base::create(nested_column_->assumeMutable(), std::move(indexes_));
    }

    bool isReplicated() const override { return true; }
    const char * getFamilyName() const override { return "Replicated"; }
    std::string getName() const override { return "Replicated(" + nested_column->getName() + ")"; }
    TypeIndex getDataType() const override { return nested_column->getDataType(); }
    MutableColumnPtr cloneResized(size_t new_size) const override;
    MutableColumnPtr cloneEmpty() const override;
    size_t size() const override { return indexes.getIndexes()->size(); }
    bool isDefaultAt(size_t n) const override;
    bool isNullAt(size_t n) const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    DataTypePtr getValueNameAndTypeImpl(WriteBufferFromOwnString & name_buf, size_t n, const IColumn::Options & options) const override;
    bool getBool(size_t n) const override;
    Float64 getFloat64(size_t n) const override;
    Float32 getFloat32(size_t n) const override;
    UInt64 getUInt(size_t n) const override;
    Int64 getInt(size_t n) const override;
    UInt64 get64(size_t n) const override;
    std::string_view getDataAt(size_t n) const override;

    ColumnPtr convertToFullColumnIfReplicated() const override;

    void insertData(const char * pos, size_t length) override;
    std::string_view serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override;
    char * serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const override;
    std::optional<size_t> getSerializedValueSize(size_t n, const IColumn::SerializationSettings * settings) const override;
    void deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) override;
    void skipSerializedInArena(ReadBuffer & in) const override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
#endif
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertManyFrom(const IColumn & src, size_t n, size_t length) override;
#else
    void doInsertManyFrom(const IColumn & src, size_t n, size_t length) override;
#endif
    void insertDefault() override;
    void insertManyDefaults(size_t length) override;

    void popBack(size_t n) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & res_indexes, size_t limit) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const override;
    bool hasEqualValues() const override;


    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

    struct ComparatorCollationBase;

    using ComparatorCollationAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationAscendingStable = ComparatorAscendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingStable = ComparatorDescendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationEqual = ComparatorEqualImpl<ComparatorCollationBase>;

    void getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;

    void getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges& equal_ranges) const override;

    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    WeakHash32 getWeakHash32() const override;
    void updateHashFast(SipHash & hash) const override;
    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;

    void getIndicesOfNonDefaultRows(Offsets & result_indexes, size_t from, size_t limit) const override;
    UInt64 getNumberOfDefaultRows() const override;

    ColumnPtr compress(bool force_compression) const override;

    ColumnCheckpointPtr getCheckpoint() const override;
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override;
    void rollback(const ColumnCheckpoint & checkpoint) override;

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;

    bool structureEquals(const IColumn & rhs) const override;

    bool isNullable() const override { return nested_column->isNullable(); }
    bool isFixedAndContiguous() const override { return false; }
    bool valuesHaveFixedSize() const override { return nested_column->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return nested_column->sizeOfValueIfFixed(); }
    bool isCollationSupported() const override { return nested_column->isCollationSupported(); }

    bool hasDynamicStructure() const override { return nested_column->hasDynamicStructure(); }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns, std::optional<size_t> max_dynamic_subcolumns) override;
    void takeDynamicStructureFromColumn(const ColumnPtr & source_column) override;
    void fixDynamicStructure() override { nested_column->fixDynamicStructure(); }

    const ColumnIndex & getIndexes() const { return indexes; }
    ColumnIndex & getIndexes() { return indexes; }

    const ColumnPtr & getIndexesColumn() const { return indexes.getIndexes(); }

    const ColumnPtr & getNestedColumn() const { return nested_column; }
    WrappedPtr & getNestedColumn() { return nested_column; }

private:
    WrappedPtr nested_column;
    ColumnIndex indexes;

    /// Unique id taken from static global_id_counter field at creation.
    /// It's used as the key in the insertion cache.
    UInt64 id;
    /// During inserts into ColumnReplicated from another ColumnReplicated we remember
    /// what values at what index we already inserted to avoid copying of these values on each call
    /// of insertFrom/insertRangeFrom/insertManyFrom.
    /// It helps to reduce memory usage during sorting/merge-sorting of replicated columns where
    /// we create empty ColumnReplicated and do insertFrom/insertRangeFrom/insertManyFrom from
    /// source columns.
    /// Mapping is the following: id -> (source_index -> inserted_index).
    std::unordered_map<UInt64, std::unordered_map<size_t, size_t>> insertion_cache;

    /// Global counter used to create a unique id for each ColumnReplicated instance.
    static std::atomic<UInt64> global_id_counter;
};

ColumnPtr recursiveRemoveReplicated(const ColumnPtr & column);
ColumnPtr convertOffsetsToIndexes(const IColumn::Offsets & offsets);

/// For some columns like Const/LowCardinality/Int* lazy replication is useless and can lead to worse performance.
bool isLazyReplicationUseful(const ColumnPtr & column);

}
