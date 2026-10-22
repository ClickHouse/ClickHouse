#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <atomic>

#include "config.h"


class Collator;

namespace DB
{

using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap *;

/// Class that specifies nullable columns. A nullable column represents
/// a column, which may have any type, provided with the possibility of
/// storing NULL values. For this purpose, a ColumNullable object stores
/// an ordinary column along with a special column, namely a byte map,
/// whose type is ColumnUInt8. The latter column indicates whether the
/// value of a given row is a NULL or not. Such a design is preferred
/// over a bitmap because columns are usually stored on disk as compressed
/// files. In this regard, using a bitmap instead of a byte map would
/// greatly complicate the implementation with little to no benefits.
///
/// The null map is optional: a column that is known to have no NULLs may hold no null map at all,
/// which saves memory and lets the algorithms skip the null handling. Use `hasNullMap`,
/// `tryGetNullMapData` and `tryGetNullMapColumnPtr` to take advantage of it. The methods returning
/// the null map by reference (`getNullMapData`, `getNullMapColumn`, `getNullMapColumnPtr`) and the
/// non-const methods returning the nested column (`getNestedColumn`, `getNestedColumnPtr`) create
/// a zero-filled null map on demand, so the code not aware of the optional null map keeps working.
/// The creation from the const methods is thread-safe, as the column may be shared between threads.
class ColumnNullable final : public COWHelper<IColumnHelper<ColumnNullable>, ColumnNullable>
{
private:
    friend class COWHelper<IColumnHelper<ColumnNullable>, ColumnNullable>;

    /// `null_map_` can be nullptr, which means the column has no NULLs.
    ColumnNullable(MutableColumnPtr && nested_column_, MutableColumnPtr && null_map_);
    /// Creates a column without NULLs and without a null map.
    explicit ColumnNullable(MutableColumnPtr && nested_column_);
    ColumnNullable(const ColumnNullable & other);

public:
    static constexpr UInt8 IS_NULL_MASK = 1;
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnNullable>, ColumnNullable>;
    /// `null_map_` can be nullptr, which means the column has no NULLs.
    static Ptr create(const ColumnPtr & nested_column_, const ColumnPtr & null_map_)
    {
        if (!null_map_)
            return ColumnNullable::create(nested_column_->assumeMutable());
        return ColumnNullable::create(nested_column_->assumeMutable(), null_map_->assumeMutable());
    }

    /// Creates a column without NULLs and without a null map.
    static Ptr create(const ColumnPtr & nested_column_)
    {
        return ColumnNullable::create(nested_column_->assumeMutable());
    }

    template <typename ... Args>
    requires (IsMutableColumns<Args ...>::value)
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }

    const char * getFamilyName() const override { return "Nullable"; }
    std::string getName() const override { return "Nullable(" + nested_column->getName() + ")"; }
    TypeIndex getDataType() const override { return TypeIndex::Nullable; }
    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const final
    {
        if (const auto * map = tryGetNullMapColumn())
            return map->size();
        return nested_column->size();
    }

    bool isNullAt(size_t n) const final
    {
        const auto * map = tryGetNullMapData();
        return map && (*map)[n] != 0;
    }
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    void getValueNameImpl(WriteBufferFromOwnString &, size_t n, const Options &) const override;
    bool getBool(size_t n) const override { return isNullAt(n) ? false : nested_column->getBool(n); }
    UInt64 get64(size_t n) const override { return nested_column->get64(n); }
    Float64 getFloat64(size_t n) const override;
    Float32 getFloat32(size_t n) const override;
    UInt64 getUInt(size_t n) const override;
    Int64 getInt(size_t n) const override;
    bool isDefaultAt(size_t n) const override { return isNullAt(n); }

    bool hasOnlyTypeDefaults() const override;
    std::string_view getDataAt(size_t) const override;
    bool supportsGetDataAt() const override { return nested_column->supportsGetDataAt(); }
    /// Will insert null value if pos=nullptr
    void insertData(const char * pos, size_t length) override;
    std::string_view serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override;
    char * serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const override;
    std::optional<size_t> getSerializedValueSize(size_t n, const IColumn::SerializationSettings * settings) const override;
    void deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
#endif

    void insertFromNotNullable(const IColumn & src, size_t n);
    void insertRangeFromNotNullable(const IColumn & src, size_t start, size_t length);
    void insertManyFromNotNullable(const IColumn & src, size_t position, size_t length);

    void insertDefault() override
    {
        materializeNullMap();
        nested_column->insertDefault();
        /// Keep the nested column and the null map in sync even if appending to the
        /// null map throws (e.g. on a memory limit).
        try
        {
            getNullMapData().push_back(true);
        }
        catch (...)
        {
            nested_column->popBack(1);
            throw;
        }
    }

    void popBack(size_t n) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
#endif

#if USE_EMBEDDED_COMPILER

    bool isComparatorCompilable() const override;

    llvm::Value * compileComparator(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*lhs*/, llvm::Value * /*rhs*/, llvm::Value * /*nan_direction_hint*/) const override;

#endif

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator &) const override;
    size_t getEqualRangeEndAssumeSorted(size_t begin, size_t end, int nan_direction_hint) const override;
    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int null_direction_hint, Permutation & res) const override;
    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;
    void getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int null_direction_hint, Permutation & res) const override;
    void updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int null_direction_hint, Permutation & res, EqualRanges& equal_ranges) const override;
    size_t estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const override;
    void reserve(size_t n) override;
    size_t capacity() const override;
    void prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor) override;
    void shrinkToFit() override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateHashWithValueRange(size_t begin, size_t end, SipHash & hash) const override;
    void computeHashInto(size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const override;
    void updateHashFast(SipHash & hash) const override;
    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;
    // Special function for nullable minmax index
    void getExtremesNullLast(Field & min, Field & max, size_t start, size_t end) const;

    ColumnPtr compress(bool force_compression) const override;

    ColumnCheckpointPtr getCheckpoint() const override;
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override;
    void rollback(const ColumnCheckpoint & checkpoint) override;

    /// The null map can be created concurrently by another thread, so the generic implementation,
    /// which visits the subcolumns twice, could see a different set of subcolumns in the second pass.
    /// This implementation takes a single snapshot of the null map.
    [[nodiscard]] ColumnPtr convertToFullIfWrapped() const override;

    /// The null map is visited only if it is present.
    void forEachMutableSubcolumn(MutableColumnCallback callback) override
    {
        callback(nested_column);
        if (hasNullMap())
            callback(null_map);
    }

    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*nested_column);
        nested_column->forEachMutableSubcolumnRecursively(callback);
        if (hasNullMap())
        {
            callback(*null_map);
            null_map->forEachMutableSubcolumnRecursively(callback);
        }
    }

    void forEachSubcolumn(ColumnCallback callback) const override
    {
        callback(nested_column);
        if (hasNullMap())
            callback(null_map);
    }

    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override
    {
        callback(*nested_column);
        nested_column->forEachSubcolumnRecursively(callback);
        if (hasNullMap())
        {
            callback(*null_map);
            null_map->forEachSubcolumnRecursively(callback);
        }
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_nullable = typeid_cast<const ColumnNullable *>(&rhs))
            return nested_column->structureEquals(*rhs_nullable->nested_column);
        return false;
    }

    ColumnPtr createWithOffsets(const Offsets & offsets, const ColumnConst & column_with_default_value, size_t total_rows, size_t shift) const override;
    void updateAt(const IColumn & src, size_t dst_pos, size_t src_pos) override;

    bool isNullable() const override { return true; }
    ColumnPlanes getPlanes() const override;
    bool isFixedAndContiguous() const override { return false; }
    bool valuesHaveFixedSize() const override { return nested_column->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return sizeof(UInt8) + nested_column->sizeOfValueIfFixed(); }
    bool onlyNull() const override { return nested_column->isDummy(); }
    bool isCollationSupported() const override { return nested_column->isCollationSupported(); }

    void serializeAsComparable(size_t n, String & out) const override;
    void batchSerializeAsComparable(size_t num_rows, VectorWithMemoryTracking<String> & out, const IColumn::Permutation * permutation, const UInt8 * null_map) const override;


    /// Return the column that represents values.
    /// The non-const variants create the null map if it is absent: the caller may change the size
    /// of the nested column, and the null map would not be possible to create correctly after that.
    IColumn & getNestedColumn() { materializeNullMap(); return *nested_column; }
    const IColumn & getNestedColumn() const { return *nested_column; }

    const ColumnPtr & getNestedColumnPtr() const { return nested_column; }
    ColumnPtr & getNestedColumnPtr() { materializeNullMap(); return nested_column; }

    /// Return the nested column without creating the null map. If the column has no null map,
    /// the rows inserted into the nested column are not NULL.
    IColumn & getNestedColumnNoMaterialize() { return *nested_column; }

    /// Whether the column has a null map. If it does not, the column has no NULLs.
    /// If it does, the column may or may not have NULLs.
    bool hasNullMap() const { return has_null_map.load(std::memory_order_acquire); }

    /// Return the null map, or nullptr if the column has no null map (and so has no NULLs).
    const ColumnUInt8 * tryGetNullMapColumn() const
    {
        return hasNullMap() ? &assert_cast<const ColumnUInt8 &>(*null_map) : nullptr;
    }

    ConstNullMapPtr tryGetNullMapData() const
    {
        const auto * map = tryGetNullMapColumn();
        return map ? &map->getData() : nullptr;
    }

    /// Return the data of the null map, or a pointer to zeros if the column has no null map.
    /// For the code reading the null map by a raw pointer. The null map is created only for huge columns.
    const UInt8 * getNullMapRawDataOrZeros() const;

    /// Can be passed to `ColumnNullable::create` to create a column with the same NULLs.
    ColumnPtr tryGetNullMapColumnPtr() const { return hasNullMap() ? ColumnPtr(null_map) : ColumnPtr(); }

    /// Return the column that represents the byte map. These methods create the null map if it is absent.
    const ColumnPtr & getNullMapColumnPtr() const { materializeNullMap(); return null_map; }
    ColumnPtr & getNullMapColumnPtr() { materializeNullMap(); return null_map; }

    ColumnUInt8 & getNullMapColumn() { materializeNullMap(); return assert_cast<ColumnUInt8 &>(*null_map); }
    const ColumnUInt8 & getNullMapColumn() const { materializeNullMap(); return assert_cast<const ColumnUInt8 &>(*null_map); }

    NullMap & getNullMapData() { return getNullMapColumn().getData(); }
    const NullMap & getNullMapData() const { return getNullMapColumn().getData(); }

    /// Create a zero-filled null map if the column has no null map.
    void materializeNullMap() const
    {
        if (!hasNullMap()) [[unlikely]]
            materializeNullMapSlow();
    }

    /// Remove the null map if it has no NULLs. Returns whether the column has no null map after that.
    bool dropNullMapIfNoNulls();

    ColumnPtr getNestedColumnWithDefaultOnNull() const;

    /// Apply the null byte map of a specified nullable column onto the
    /// null byte map of the current column by performing an element-wise OR
    /// between both byte maps. This method is used to determine the null byte
    /// map of the result column of a function taking one or more nullable
    /// columns.
    void applyNullMap(const ColumnNullable & other);
    void applyNullMap(const ColumnUInt8 & map);
    void applyNullMap(const NullMap & map);
    void applyNegatedNullMap(const ColumnUInt8 & map);
    /// When `offset` is given, only the rows in `[offset, offset + map.size())` are affected and `map`
    /// covers just that range; otherwise it must cover the whole column.
    void applyNegatedNullMap(const NullMap & map, size_t offset = 0);

    /// Check that size of null map equals to size of nested column.
    void checkConsistency() const;

    bool hasDynamicStructure() const override { return nested_column->hasDynamicStructure(); }
    void takeExactDynamicStructureFrom(const IColumn & source) override;
    void chooseDynamicStructureForMerge(const VectorWithMemoryTracking<ColumnPtr> & source_columns, std::optional<size_t> max_dynamic_subcolumns) override;
    void fixDynamicStructure() override { nested_column->fixDynamicStructure(); }
    bool dynamicStructureEquals(const IColumn & rhs) const override;
    bool hasStatistics() const override { return nested_column->hasStatistics(); }
    void takeOrCalculateStatisticsFrom(const VectorWithMemoryTracking<ColumnPtr> & source_columns) override;

    void fillFromRowStorePtrs(const DataTypePtr & type, const RowStorePointers & row_store_ptrs, size_t field_offset, size_t field_size, size_t begin, size_t count, PaddedPODArray<UInt8> * null_map) override;

private:
    WrappedPtr nested_column;

    /// nullptr if the column has no NULLs. It is mutable, because it can be created on demand
    /// in the const methods. It is read only after `has_null_map` is checked to be true,
    /// so its creation by one thread does not race with the readers in other threads.
    mutable WrappedPtr null_map;
    mutable std::atomic<bool> has_null_map = false;

    void materializeNullMapSlow() const;

    template <bool negative>
    void applyNullMapImpl(const NullMap & map, size_t offset = 0);

    /// Probe the nested column's comparable serialization once so unsupported nested
    /// types are rejected even on all-NULL blocks. Shared by the single-row and batch paths.
    void validateNestedComparable() const;

    int compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint, const Collator * collator=nullptr) const;

    void getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int null_direction_hint, Permutation & res, const Collator * collator = nullptr) const;

    void updatePermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                            size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_ranges, const Collator * collator = nullptr) const;
};

ColumnPtr makeNullable(const ColumnPtr & column);
ColumnPtr makeNullableSafe(const ColumnPtr & column);
ColumnConstPtr makeNullableSafe(const ColumnConstPtr & column);
ColumnPtr makeNullableOrLowCardinalityNullable(const ColumnPtr & column);
ColumnPtr makeNullableOrLowCardinalityNullableSafe(const ColumnPtr & column);

ColumnPtr removeNullable(const ColumnPtr & column);
ColumnPtr removeNullableOrLowCardinalityNullable(const ColumnPtr & column);

/// Create the missing null maps of `column` and its subcolumns, if they are `ColumnNullable`.
/// The readers of a shared `Nullable` column may create its null map lazily, which changes `allocatedBytes`
/// and `byteSize` of the column. Call it for the columns that are retained and accounted (e.g. stored
/// in a table, in a hash join, or in a cache) before they are accounted and shared with the readers.
void materializeNullMapsRecursively(const IColumn & column);

}
