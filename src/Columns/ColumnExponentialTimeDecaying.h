#pragma once

#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>

namespace DB
{

class ColumnExponentialTimeDecaying final
    : public COWHelper<IColumnHelper<ColumnExponentialTimeDecaying>, ColumnExponentialTimeDecaying>
{
private:
    friend class COWHelper<IColumnHelper<ColumnExponentialTimeDecaying>, ColumnExponentialTimeDecaying>;

    WrappedPtr storage;
    WrappedPtr ordering_key;
    Float64 decay_length;

    ColumnExponentialTimeDecaying(MutableColumnPtr && storage_, Float64 decay_length_);
    ColumnExponentialTimeDecaying(
        MutableColumnPtr && storage_,
        MutableColumnPtr && ordering_key_,
        Float64 decay_length_);

    void appendOrderingKey(size_t row);
    void rebuildOrderingKey();

public:
    using Base = COWHelper<IColumnHelper<ColumnExponentialTimeDecaying>, ColumnExponentialTimeDecaying>;

    static MutablePtr create(MutableColumnPtr && storage_, Float64 decay_length_)
    {
        return Base::create(std::move(storage_), decay_length_);
    }

    static MutablePtr createWithPrefix(
        MutableColumnPtr && storage_,
        MutableColumnPtr && ordering_key_,
        Float64 decay_length_)
    {
        return Base::create(
            std::move(storage_),
            std::move(ordering_key_),
            decay_length_);
    }

    const char * getFamilyName() const override { return "ExponentialTimeDecaying"; }
    TypeIndex getDataType() const override { return TypeIndex::Tuple; }
    std::string getName() const override;

    MutableColumnPtr cloneResized(size_t new_size) const override;
    size_t size() const override { return storage->size(); }
    bool canBeInsideNullable() const override { return true; }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    void getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const override;
    std::string_view getDataAt(size_t n) const override { return storage->getDataAt(n); }

    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    bool isDefaultAt(size_t n) const override { return storage->isDefaultAt(n); }
    bool hasOnlyTypeDefaults() const override { return storage->hasOnlyTypeDefaults(); }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#else
    void doInsertFrom(const IColumn & src_, size_t n) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#endif

    void insertDefault() override;
    void popBack(size_t n) override;

    std::string_view serializeValueIntoArena(
        size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override;

    char * serializeValueIntoMemory(
        size_t n, char * memory, const IColumn::SerializationSettings * settings) const override;

    std::optional<size_t> getSerializedValueSize(
        size_t, const IColumn::SerializationSettings *) const override
    {
        return sizeof(UInt64);
    }

    void collectSerializedValueSizes(
        PaddedPODArray<UInt64> & sizes,
        const UInt8 * is_null,
        const IColumn::SerializationSettings * settings) const override;

    void deserializeAndInsertFromArena(
        ReadBuffer & in, const IColumn::SerializationSettings * settings) override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    void computeHashInto(size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const override;

    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr compress(bool force_compression) const override;

    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;
    void getPermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override;

    void updatePermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override;

    void reserve(size_t n) override
    {
        storage->reserve(n);
        ordering_key->reserve(n);
    }

    void prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor) override;

    void shrinkToFit() override
    {
        storage->shrinkToFit();
        ordering_key->shrinkToFit();
    }

    void ensureOwnership() override
    {
        storage->ensureOwnership();
        ordering_key->ensureOwnership();
    }

    void protect() override
    {
        storage->protect();
        ordering_key->protect();
    }

    size_t capacity() const override { return storage->capacity(); }
    size_t byteSize() const override { return storage->byteSize() + ordering_key->byteSize(); }
    size_t byteSizeAt(size_t n) const override { return storage->byteSizeAt(n) + ordering_key->byteSizeAt(n); }
    size_t allocatedBytes() const override { return storage->allocatedBytes() + ordering_key->allocatedBytes(); }
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override { storage->updateCheckpoint(checkpoint); }
    void rollback(const ColumnCheckpoint & checkpoint) override;
    ColumnCheckpointPtr getCheckpoint() const override { return storage->getCheckpoint(); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;

    void finalize() override
    {
        storage->finalize();
        ordering_key->finalize();
    }

    bool isFinalized() const override { return storage->isFinalized() && ordering_key->isFinalized(); }

    bool structureEquals(const IColumn & rhs) const override;

    Float64 getDecayLength() const { return decay_length; }
    const IColumn & getStorageColumn() const { return *storage; }
    IColumn & getStorageColumn() { return *storage; }
    const ColumnTuple & getStorageTuple() const { return assert_cast<const ColumnTuple &>(*storage); }
    ColumnTuple & getStorageTuple() { return assert_cast<ColumnTuple &>(*storage); }
    const ColumnPtr & getStoragePtr() const { return storage; }
    const IColumn & getOrderingKeyColumn() const { return *ordering_key; }

    /// The serialized/direct payload can be appended independently of the derived
    /// ordering key. Synchronize the cache after such a deserialization.
    void syncOrderingKeyFrom(size_t previous_size);
};

}
