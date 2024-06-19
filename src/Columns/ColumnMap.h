#pragma once

#include <Core/Block.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include "Common/assert_cast.h"
#include "Columns/IColumn.h"

namespace DB
{

/** Column, that stores a nested Array(Tuple(key, value)) column.
 */
class ColumnMap final : public COWHelper<IColumnHelper<ColumnMap>, ColumnMap>
{
private:
    friend class COWHelper<IColumnHelper<ColumnMap>, ColumnMap>;

    std::vector<WrappedPtr> shards;

    ColumnMap(const ColumnMap &) = default;
    explicit ColumnMap(MutableColumns && shards_);

    template <typename F> MutableColumns applyForShards(F && f) const;
    std::vector<WrappedPtr> cloneEmptyShards() const;

    static void concatToOneShard(std::vector<WrappedPtr> && shard_sources, IColumn & res);

    template <bool one_value, typename Inserter>
    void insertRangeImpl(const ColumnMap & src, Inserter && inserter);

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnMap>, ColumnMap>;

    static Ptr create(Columns && columns);
    static Ptr create(const ColumnPtr & column) { return ColumnMap::create(column->assumeMutable()); }
    static Ptr create(ColumnPtr && column) { return ColumnMap::create(column->assumeMutable()); }
    static Ptr create(const ColumnPtr & keys, const ColumnPtr & values, const ColumnPtr & offsets);

    static MutablePtr create(MutableColumns && columns) { return Base::create(std::move(columns)); }
    static MutablePtr create(MutableColumnPtr && column);

    size_t getNumShards() const { return shards.size(); }
    Columns getShards() const { return Columns(shards.begin(), shards.end()); }

    const ColumnPtr & getShardPtr(size_t idx) const { return shards[idx]; }
    ColumnPtr & getShardPtr(size_t idx) { return shards[idx]; }

    const ColumnArray & getShard(size_t idx) const { return assert_cast<const ColumnArray &>(*shards[idx]); }
    ColumnArray & getShard(size_t idx) { return assert_cast<ColumnArray &>(*shards[idx]); }

    const ColumnTuple & getShardData(size_t idx) const { return assert_cast<const ColumnTuple &>(getShard(idx).getData()); }
    ColumnTuple & getShardData(size_t idx) { return assert_cast<ColumnTuple &>(getShard(idx).getData()); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Map"; }
    TypeIndex getDataType() const override { return TypeIndex::Map; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { return shards[0]->size(); }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    bool isDefaultAt(size_t n) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;
    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;
    void reserve(size_t n) override;
    void shrinkToFit() override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(MutableColumnCallback callback) override;
    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;
    ColumnPtr finalize() const override;
    bool isFinalized() const override;

    const ColumnArray & getNestedColumn() const { return assert_cast<const ColumnArray &>(*shards[0]); }
    ColumnArray & getNestedColumn() { return assert_cast<ColumnArray &>(*shards[0]); }

    const ColumnPtr & getNestedColumnPtr() const { return shards[0]; }
    ColumnPtr & getNestedColumnPtr() { return shards[0]; }

    const ColumnTuple & getNestedData() const { return assert_cast<const ColumnTuple &>(getNestedColumn().getData()); }
    ColumnTuple & getNestedData() { return assert_cast<ColumnTuple &>(getNestedColumn().getData()); }

    ColumnPtr compress() const override;

    bool hasDynamicStructure() const override { return shards[0]->hasDynamicStructure(); }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override;
};

}
