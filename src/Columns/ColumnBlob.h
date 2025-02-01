#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/WeakHash.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


class ColumnBlob : public COWHelper<IColumnHelper<ColumnBlob>, ColumnBlob>
{
public:
    using Lazy = std::function<ColumnPtr()>;

    explicit ColumnBlob(ColumnPtr src_col_) : src_col(std::move(src_col_)) { }

    const char * getFamilyName() const override { return "Blob"; }

    size_t size() const override { return src_col->size(); }
    size_t byteSize() const override { return src_col->byteSize(); }
    size_t allocatedBytes() const override { return src_col->allocatedBytes(); }

    /// Helper methods for compression / decompression.

    /// If data is not worth to be compressed - returns nullptr.
    /// By default it requires that compressed data is at least 50% smaller than original.
    /// With `force_compression` set to true, it requires compressed data to be not larger than the source data.
    /// Note: shared_ptr is to allow to be captured by std::function.
    static std::shared_ptr<Memory<>> compressBuffer(const void * data, size_t data_size, bool force_compression);

    static void decompressBuffer(const void * compressed_data, void * decompressed_data, size_t compressed_size, size_t decompressed_size);

    /// All other methods throw the exception.

    TypeIndex getDataType() const override { throwInapplicable(); }
    Field operator[](size_t) const override { throwInapplicable(); }
    void get(size_t, Field &) const override { throwInapplicable(); }
    std::pair<String, DataTypePtr> getValueNameAndType(size_t) const override { throwInapplicable(); }
    StringRef getDataAt(size_t) const override { throwInapplicable(); }
    bool isDefaultAt(size_t) const override { throwInapplicable(); }
    void insert(const Field &) override { throwInapplicable(); }
    bool tryInsert(const Field &) override { throwInapplicable(); }
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn &, size_t, size_t) override { throwInapplicable(); }
#else
    void doInsertRangeFrom(const IColumn &, size_t, size_t) override { throwInapplicable(); }
#endif
    void insertData(const char *, size_t) override { throwInapplicable(); }
    void insertDefault() override { throwInapplicable(); }
    void popBack(size_t) override { throwInapplicable(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwInapplicable(); }
    char * serializeValueIntoMemory(size_t, char *) const override { throwInapplicable(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwInapplicable(); }
    const char * skipSerializedInArena(const char *) const override { throwInapplicable(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwInapplicable(); }
    WeakHash32 getWeakHash32() const override { throwInapplicable(); }
    void updateHashFast(SipHash &) const override { throwInapplicable(); }
    ColumnPtr filter(const Filter &, ssize_t) const override { throwInapplicable(); }
    void expand(const Filter &, bool) override { throwInapplicable(); }
    ColumnPtr permute(const Permutation &, size_t) const override { throwInapplicable(); }
    ColumnPtr index(const IColumn &, size_t) const override { throwInapplicable(); }
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn &, int) const override { throwInapplicable(); }
#else
    int doCompareAt(size_t, size_t, const IColumn &, int) const override { throwInapplicable(); }
#endif
    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
        throwInapplicable();
    }
    bool hasEqualValues() const override { throwInapplicable(); }
    void getPermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability, size_t, int, Permutation &) const override
    {
        throwInapplicable();
    }
    void updatePermutation(
        IColumn::PermutationSortDirection, IColumn::PermutationSortStability, size_t, int, Permutation &, EqualRanges &) const override
    {
        throwInapplicable();
    }
    ColumnPtr replicate(const Offsets &) const override { throwInapplicable(); }
    MutableColumns scatter(ColumnIndex, const Selector &) const override { throwInapplicable(); }
    void gather(ColumnGathererStream &) override { throwInapplicable(); }
    void getExtremes(Field &, Field &) const override { throwInapplicable(); }
    size_t byteSizeAt(size_t) const override { throwInapplicable(); }
    double getRatioOfDefaultRows(double) const override { throwInapplicable(); }
    UInt64 getNumberOfDefaultRows() const override { throwInapplicable(); }
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override { throwInapplicable(); }

    bool hasDynamicStructure() const override { throwInapplicable(); }
    void takeDynamicStructureFromSourceColumns(const Columns &) override { throwInapplicable(); }

private:
    // Maybe we shouldn't keep this reference
    ColumnPtr src_col;

    [[noreturn]] static void throwInapplicable()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBlob should be converted to a regular column before usage");
    }
};

}
