#pragma once

#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>

namespace DB
{

class ColumnExponentialTimeDecaying final
    : public COWHelper<IColumnHelper<ColumnExponentialTimeDecaying>, ColumnExponentialTimeDecaying>
{
private:
    friend class COWHelper<IColumnHelper<ColumnExponentialTimeDecaying>, ColumnExponentialTimeDecaying>;

    WrappedPtr storage;
    Float64 decay_length;

    ColumnExponentialTimeDecaying(MutableColumnPtr && storage_, Float64 decay_length_);

    int compareDirect(size_t n, size_t m, const ColumnExponentialTimeDecaying & rhs) const;

public:
    using Base = COWHelper<IColumnHelper<ColumnExponentialTimeDecaying>, ColumnExponentialTimeDecaying>;

    static MutablePtr create(MutableColumnPtr && storage_, Float64 decay_length_)
    {
        return Base::create(std::move(storage_), decay_length_);
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

    void insertData(const char * pos, size_t length) override { storage->insertData(pos, length); }
    void insert(const Field & x) override { storage->insert(x); }
    bool tryInsert(const Field & x) override { return storage->tryInsert(x); }
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

    void insertDefault() override { storage->insertDefault(); }
    void popBack(size_t n) override { storage->popBack(n); }

    std::string_view serializeValueIntoArena(
        size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override
    {
        return storage->serializeValueIntoArena(n, arena, begin, settings);
    }

    char * serializeValueIntoMemory(
        size_t n, char * memory, const IColumn::SerializationSettings * settings) const override
    {
        return storage->serializeValueIntoMemory(n, memory, settings);
    }

    void deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) override
    {
        storage->deserializeAndInsertFromArena(in, settings);
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override { storage->updateHashWithValue(n, hash); }
    void updateHashFast(SipHash & hash) const override { storage->updateHashFast(hash); }
    void computeHashInto(size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const override
    {
        storage->computeHashInto(row_begin, row_end, hash_out, initial);
    }

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

    void reserve(size_t n) override { storage->reserve(n); }
    void prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor) override;
    void shrinkToFit() override { storage->shrinkToFit(); }
    void ensureOwnership() override { storage->ensureOwnership(); }
    void protect() override { storage->protect(); }

    size_t capacity() const override { return storage->capacity(); }
    size_t byteSize() const override { return storage->byteSize(); }
    size_t byteSizeAt(size_t n) const override { return storage->byteSizeAt(n); }
    size_t allocatedBytes() const override { return storage->allocatedBytes(); }
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override { storage->updateCheckpoint(checkpoint); }
    void rollback(const ColumnCheckpoint & checkpoint) override { storage->rollback(checkpoint); }
    ColumnCheckpointPtr getCheckpoint() const override { return storage->getCheckpoint(); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;
    void finalize() override { storage->finalize(); }
    bool isFinalized() const override { return storage->isFinalized(); }

    bool structureEquals(const IColumn & rhs) const override;

    Float64 getDecayLength() const { return decay_length; }
    const IColumn & getStorageColumn() const { return *storage; }
    IColumn & getStorageColumn() { return *storage; }
    const ColumnTuple & getStorageTuple() const { return assert_cast<const ColumnTuple &>(*storage); }
    ColumnTuple & getStorageTuple() { return assert_cast<ColumnTuple &>(*storage); }
    const ColumnPtr & getStoragePtr() const { return storage; }
};

}
