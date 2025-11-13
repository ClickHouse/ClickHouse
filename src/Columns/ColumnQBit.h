#pragma once

#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>

#include <base/StringRef.h>


namespace DB
{

/** Column that represents bit-transposed vectors for efficient vector search operations.
  *
  * This column type stores array data in a bit-transposed layout, where bits are grouped by position
  * rather than by vector element. For example, with Float32 vectors, there are 32 groups (one for each bit),
  * and each group contains the corresponding bit from all vector elements.
  *
  * This column is designed to store the output of the transposeBits() function calls, which convert one float within
  * a regular array into this bit-transposed format. Currently supported numeric types include:
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
  * │ QBit │────▶│Tuple(FixedString, FixedString, ... x32) │
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
class ColumnQBit final : public COWHelper<IColumnHelper<ColumnQBit>, ColumnQBit>
{
private:
    friend class COWHelper<IColumnHelper<ColumnQBit>, ColumnQBit>;

    /// The actual storage for the data
    WrappedPtr tuple;
    /// Number of elements in the original vectors. We will store dimension elements padded to a multiple of 8 (padding elements are 0)
    size_t dimension = 0;

    explicit ColumnQBit(MutableColumnPtr && tuple_, size_t dimension);


public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnQBit>, ColumnQBit>;

    static Ptr create(const ColumnPtr & column, size_t dimension) { return Base::create(column->assumeMutable(), dimension); }
    static MutablePtr create(MutableColumnPtr && tuple_, size_t dimension) { return Base::create(std::move(tuple_), dimension); }

    const char * getFamilyName() const override { return "QBit"; }
    TypeIndex getDataType() const override { return TypeIndex::QBit; }
    std::string getName() const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;
    bool canBeInsideNullable() const override { return true; }

    /// Number of rows
    size_t size() const override { return tuple->size(); }
    /// Number of columns in the tuple, which corresponds to the number of bit groups
    size_t getBitsCount() const;
    /// Number of elements in the vectors
    size_t getDimension() const { return dimension; }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    DataTypePtr getValueNameAndTypeImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const override;

    StringRef getDataAt(size_t n) const override { return tuple->getDataAt(n); }
    void insertData(const char * pos, size_t length) override { tuple->insertData(pos, length); }
    void insert(const Field & x) override { tuple->insert(x); }
    bool tryInsert(const Field & x) override { return tuple->tryInsert(x); }
    bool isDefaultAt(size_t n) const override { return tuple->isDefaultAt(n); }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertFrom(const IColumn & src_, size_t n) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn &, int) const override
#else
    int doCompareAt(size_t, size_t, const IColumn &, int) const override
#endif
    {
        return 0;
    }

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
    void updateHashFast(SipHash & hash) const override { tuple->updateHashFast(hash); }
    WeakHash32 getWeakHash32() const override { return tuple->getWeakHash32(); }

    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr compress(bool force_compression) const override;

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

    void reserve(size_t n) override { tuple->reserve(n); }
    void prepareForSquashing(const Columns & source_columns, size_t factor) override;
    void shrinkToFit() override { tuple->shrinkToFit(); }
    void ensureOwnership() override { tuple->ensureOwnership(); }
    void protect() override { tuple->protect(); }

    size_t capacity() const override { return tuple->capacity(); }
    size_t byteSize() const override { return tuple->byteSize(); }
    size_t byteSizeAt(size_t n) const override { return tuple->byteSizeAt(n); }
    size_t allocatedBytes() const override { return tuple->allocatedBytes(); }
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override { tuple->updateCheckpoint(checkpoint); }
    void rollback(const ColumnCheckpoint & checkpoint) override { tuple->rollback(checkpoint); }
    ColumnCheckpointPtr getCheckpoint() const override { return tuple->getCheckpoint(); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;
    void finalize() override { tuple->finalize(); }

    bool structureEquals(const IColumn & rhs) const override { return tuple->structureEquals(rhs); }
    bool isFinalized() const override { return tuple->isFinalized(); }

    /// Efficient access to the underlying tuple
    const ColumnPtr & getTuple() const { return tuple; }
    const IColumn & getTupleColumn() const { return *tuple; }
    const ColumnTuple & getNestedData() const { return assert_cast<const ColumnTuple &>(getTupleColumn()); }
    ColumnPtr & getTuple() { return tuple; }
    IColumn & getTupleColumn() { return *tuple.get(); }
    ColumnTuple & getNestedData() { return assert_cast<ColumnTuple &>(getTupleColumn()); }
};

}
