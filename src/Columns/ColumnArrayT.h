#pragma once

#include <string>
#include <base/StringRef.h>
#include <base/types.h>

#include <Columns/IColumn.h>

#include <Common/WeakHash.h>

#include <Core/Field.h>


namespace DB
{

/** Column that represents bit-transposed vectors for efficient vector search operations.
  *
  * This column type stores array data in a bit-transposed layout, where bits are grouped by position
  * rather than by vector element. For example, with Float32 vectors, there are 32 groups (one for each bit),
  * and each group contains the corresponding bit from all vector elements.
  *
  * This column is designed to store the output of the transposeBits() function, which converts
  * regular arrays into this bit-transposed format. Currently supported numeric types include:
  * - Float64 (64 bit groups)
  * - Float32 (32 bit groups)
  * - BFloat16 (16 bit groups)
  *
  * Internal structure:
  * - For a Float32 array, the underlying storage is a tuple of 32 FixedString columns
  * - Each FixedString column stores all bits from a specific bit position (0-31)
  * - The length of each FixedString equals the number of vector elements (padded to a multiple of 8)
  *
  * A key optimization for vector search operations is the ability to read only p bit groups
  * (p < 32 for Float32) to reconstruct vectors with p bits of precision. This allows for:
  * - Reduced I/O operations during search
  * - Progressive refinement of search results
  * - Adjustable precision-performance tradeoffs
  *
  * Visual representation:
  *
  * ┌──────┐     ┌─────────────────────────────────────────┐
  * │ArrayT│────▶│Tuple(FixedString, FixedString, ... x32) │
  * └──────┘     └─────────────────────────────────────────┘
  *                 │            │                │
  *                 ▼            ▼                ▼
  *              ┌────┐       ┌────┐           ┌────┐
  *              │Bit0│       │Bit1│    ...    │Bit31
  *              └────┘       └────┘           └────┘
  *
  * The implementation delegates most operations to the underlying tuple column,
  * providing array-specific semantics where needed.
  */
class ColumnArrayT final : public COWHelper<IColumnHelper<ColumnArrayT>, ColumnArrayT>
{
private:
    friend class COWHelper<IColumnHelper<ColumnArrayT>, ColumnArrayT>;

    /** The actual storage for the data. */
    WrappedPtr tuple;

    explicit ColumnArrayT(MutableColumnPtr && tuple_);
    ColumnArrayT(const ColumnArrayT &) = default;

    int compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint, const Collator * collator = nullptr) const;


public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnArrayT>, ColumnArrayT>;

    static Ptr create(const ColumnPtr & column) { return ColumnArrayT::create(column->assumeMutable()); }

    template <typename... Args>
    requires(IsMutableColumns<Args...>::value)
    static MutablePtr create(Args &&... args)
    {
        return Base::create(std::forward<Args>(args)...);
    }

    const char * getFamilyName() const override { return "ArrayT"; }
    TypeIndex getDataType() const override { return TypeIndex::ArrayT; }
    std::string getName() const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;

    /// Number of rows and number of groups in the tuple
    size_t size() const override { return tuple->size(); }
    size_t groupCount() const;

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    std::pair<String, DataTypePtr> getValueNameAndType(size_t n) const override;

    bool isDefaultAt(size_t n) const override { return tuple->isDefaultAt(n); }
    StringRef getDataAt(size_t n) const override { return tuple->getDataAt(n); }
    void insertData(const char * pos, size_t length) override { tuple->insertData(pos, length); }
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override { return tuple->tryInsert(x); }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#else
    void doInsertFrom(const IColumn & src_, size_t n) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    void insertDefault() override { tuple->insertDefault(); }
    void popBack(size_t n) override { tuple->popBack(n); }
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        return tuple->serializeValueIntoArena(n, arena, begin);
    }
    char * serializeValueIntoMemory(size_t n, char * memory) const override { return tuple->serializeValueIntoMemory(n, memory); }
    const char * deserializeAndInsertFromArena(const char * pos) override { return tuple->deserializeAndInsertFromArena(pos); }
    const char * skipSerializedInArena(const char * pos) const override { return tuple->skipSerializedInArena(pos); }
    void updateHashWithValue(size_t n, SipHash & hash) const override { tuple->updateHashWithValue(n, hash); }
    WeakHash32 getWeakHash32() const override { return tuple->getWeakHash32(); }
    void updateHashFast(SipHash & hash) const override { tuple->updateHashFast(hash); }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override { return tuple->filter(filt, result_size_hint); }
    void expand(const Filter & mask, bool inverted) override { tuple->expand(mask, inverted); }
    ColumnPtr permute(const Permutation & perm, size_t limit) const override { return tuple->permute(perm, limit); }
    ColumnPtr index(const IColumn & indexes, size_t limit) const override { return tuple->index(indexes, limit); }
    ColumnPtr replicate(const Offsets & offsets) const override;

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const override
    {
        return tuple->compareAtWithCollation(n, m, rhs, nan_direction_hint, collator);
    }
    void getExtremes(Field & min, Field & max) const override { tuple->getExtremes(min, max); }
    void getPermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override
    {
        tuple->getPermutation(direction, stability, limit, nan_direction_hint, res);
    }

    void updatePermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override
    {
        tuple->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
    }

    void getPermutationWithCollation(
        const Collator & collator,
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override
    {
        tuple->getPermutationWithCollation(collator, direction, stability, limit, nan_direction_hint, res);
    }

    void updatePermutationWithCollation(
        const Collator & collator,
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override
    {
        tuple->updatePermutationWithCollation(collator, direction, stability, limit, nan_direction_hint, res, equal_ranges);
    }
    void reserve(size_t n) override { tuple->reserve(n); }
    size_t capacity() const override { return tuple->capacity(); }
    void prepareForSquashing(const Columns & source_columns) override { tuple->prepareForSquashing(source_columns); }
    void shrinkToFit() override { tuple->shrinkToFit(); }
    void ensureOwnership() override { tuple->ensureOwnership(); }
    size_t byteSize() const override { return tuple->byteSize(); }
    size_t byteSizeAt(size_t n) const override { return tuple->byteSizeAt(n); }
    size_t allocatedBytes() const override { return tuple->allocatedBytes(); }
    void protect() override { tuple->protect(); }
    ColumnCheckpointPtr getCheckpoint() const override { return tuple->getCheckpoint(); }
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override { tuple->updateCheckpoint(checkpoint); }
    void rollback(const ColumnCheckpoint & checkpoint) override { tuple->rollback(checkpoint); }
    ColumnPtr compress(bool force_compression) const override { return tuple->compress(force_compression); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override { tuple->forEachMutableSubcolumn(callback); }
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        tuple->forEachMutableSubcolumnRecursively(callback);
    }
    void forEachSubcolumn(ColumnCallback callback) const override { tuple->forEachSubcolumn(callback); }
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override { tuple->forEachSubcolumnRecursively(callback); }
    bool structureEquals(const IColumn & rhs) const override { return tuple->structureEquals(rhs); }
    bool isCollationSupported() const override { return tuple->isCollationSupported(); }
    void finalize() override { tuple->finalize(); }
    bool isFinalized() const override { return tuple->isFinalized(); }
    bool hasDynamicStructure() const override { return tuple->hasDynamicStructure(); }
    bool dynamicStructureEquals(const IColumn & rhs) const override { return tuple->dynamicStructureEquals(rhs); }

    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override
    {
        tuple->takeDynamicStructureFromSourceColumns(source_columns);
    }

    /// Efficient access to the underlying tuple
    const ColumnPtr & getTuple() const { return tuple; }
    IColumn & getTupleColumn() { return *tuple; }
    const IColumn & getTupleColumn() const { return *tuple; }
};

}
