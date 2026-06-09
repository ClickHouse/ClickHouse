#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>

namespace DB
{

/** Column that stores fixed-dimension dense vectors contiguously for exact ("FLAT") vector search.
  *
  * Unlike ColumnArray (which carries an offsets column and does not enforce a fixed dimension), a
  * vector of N elements of type T is stored as a single contiguous block of N * sizeof(T) bytes per
  * row. The underlying storage is a single ColumnFixedString with stride N * sizeof(T), so the data is
  * laid out exactly like Faiss FLAT: row i occupies bytes [i * N * sizeof(T), (i + 1) * N * sizeof(T)).
  *
  * This contiguous, offset-free layout is friendly to SIMD distance kernels (the distance functions can
  * reinterpret the raw bytes as `const T *` and stride by N), and it makes the dimension part of the type.
  *
  * The column delegates almost all operations to the wrapped ColumnFixedString. The logical "array of
  * numbers" view (e.g. the `[1, 2, 3]` text representation) is provided by SerializationVector and the
  * Array <-> Vector cast wrappers, not by this column.
  */
class ColumnDenseVector final : public COWHelper<IColumnHelper<ColumnDenseVector>, ColumnDenseVector>
{
private:
    friend class COWHelper<IColumnHelper<ColumnDenseVector>, ColumnDenseVector>;

    /// The actual contiguous storage: ColumnFixedString with stride element_size * dimension bytes.
    WrappedPtr fixed_string;
    /// Size of one vector element in bytes: 2 (BFloat16), 4 (Float32), 8 (Float64).
    size_t element_size = 0;
    /// Number of elements in each vector.
    size_t dimension = 0;

    ColumnDenseVector(MutableColumnPtr && fixed_string_, size_t element_size_, size_t dimension_);

public:
    using Base = COWHelper<IColumnHelper<ColumnDenseVector>, ColumnDenseVector>;

    static Ptr create(const ColumnPtr & column, size_t element_size, size_t dimension)
    {
        return Base::create(column->assumeMutable(), element_size, dimension);
    }
    static MutablePtr create(MutableColumnPtr && fixed_string_, size_t element_size, size_t dimension)
    {
        return Base::create(std::move(fixed_string_), element_size, dimension);
    }

    const char * getFamilyName() const override { return "Vector"; }
    TypeIndex getDataType() const override { return TypeIndex::Vector; }
    std::string getName() const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;
    bool canBeInsideNullable() const override { return true; }

    size_t size() const override { return fixed_string->size(); }
    size_t getElementSize() const { return element_size; }
    size_t getDimension() const { return dimension; }

    Field operator[](size_t n) const override { return (*fixed_string)[n]; }
    void get(size_t n, Field & res) const override { fixed_string->get(n, res); }
    void getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const override;

    std::string_view getDataAt(size_t n) const override { return fixed_string->getDataAt(n); }
    void insertData(const char * pos, size_t length) override { fixed_string->insertData(pos, length); }
    void insert(const Field & x) override { fixed_string->insert(x); }
    bool tryInsert(const Field & x) override { return fixed_string->tryInsert(x); }
    bool isDefaultAt(size_t n) const override { return fixed_string->isDefaultAt(n); }

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
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
#endif
    {
        const auto & rhs = assert_cast<const ColumnDenseVector &>(rhs_);
        return fixed_string->compareAt(n, m, rhs.getFixedStringColumn(), nan_direction_hint);
    }

    void insertDefault() override { fixed_string->insertDefault(); }
    void popBack(size_t n) override { fixed_string->popBack(n); }
    std::string_view serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override
    {
        return fixed_string->serializeValueIntoArena(n, arena, begin, settings);
    }
    char * serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const override
    {
        return fixed_string->serializeValueIntoMemory(n, memory, settings);
    }
    void deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) override
    {
        fixed_string->deserializeAndInsertFromArena(in, settings);
    }
    void skipSerializedInArena(ReadBuffer & in) const override { fixed_string->skipSerializedInArena(in); }
    void updateHashWithValue(size_t n, SipHash & hash) const override { fixed_string->updateHashWithValue(n, hash); }
    void updateHashFast(SipHash & hash) const override { fixed_string->updateHashFast(hash); }
    WeakHash32 getWeakHash32() const override { return fixed_string->getWeakHash32(); }

    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr compress(bool force_compression) const override;

    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override { fixed_string->getExtremes(min, max, start, end); }
    void getPermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override
    {
        fixed_string->getPermutation(direction, stability, limit, nan_direction_hint, res);
    }

    void updatePermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override
    {
        fixed_string->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
    }

    void reserve(size_t n) override { fixed_string->reserve(n); }
    void shrinkToFit() override { fixed_string->shrinkToFit(); }
    void ensureOwnership() override { fixed_string->ensureOwnership(); }
    void protect() override { fixed_string->protect(); }

    size_t capacity() const override { return fixed_string->capacity(); }
    size_t byteSize() const override { return fixed_string->byteSize(); }
    size_t byteSizeAt(size_t n) const override { return fixed_string->byteSizeAt(n); }
    size_t allocatedBytes() const override { return fixed_string->allocatedBytes(); }
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override { fixed_string->updateCheckpoint(checkpoint); }
    void rollback(const ColumnCheckpoint & checkpoint) override { fixed_string->rollback(checkpoint); }
    ColumnCheckpointPtr getCheckpoint() const override { return fixed_string->getCheckpoint(); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;
    void finalize() override { fixed_string->finalize(); }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_vec = typeid_cast<const ColumnDenseVector *>(&rhs))
            return element_size == rhs_vec->element_size && dimension == rhs_vec->dimension
                && fixed_string->structureEquals(*rhs_vec->fixed_string);
        return false;
    }
    bool isFinalized() const override { return fixed_string->isFinalized(); }

    /// Access to the underlying contiguous storage (used by SIMD distance kernels).
    const ColumnPtr & getFixedString() const { return fixed_string; }
    const IColumn & getFixedStringColumn() const { return *fixed_string; }
    const ColumnFixedString & getFixedStringData() const { return assert_cast<const ColumnFixedString &>(*fixed_string); }
    ColumnPtr & getFixedString() { return fixed_string; }
    IColumn & getFixedStringColumn() { return *fixed_string.get(); }
    ColumnFixedString & getFixedStringData() { return assert_cast<ColumnFixedString &>(getFixedStringColumn()); }
};

}
