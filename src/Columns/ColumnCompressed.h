#pragma once

#include <optional>
#include <Core/Field.h>
#include <Columns/IColumn.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** Wrapper for compressed column data.
  * The only supported operations are:
  * - decompress (reconstruct the source column)
  * - get size in rows or bytes.
  *
  * It is needed to implement in-memory compression
  * - to keep compressed data in Block or pass around.
  *
  * It's often beneficial to store compressed data in-memory and decompress on the fly
  * because it allows to lower memory throughput. More specifically, if:
  *
  * decompression speed * num CPU cores >= memory read throughput
  *
  * Also in-memory compression allows to keep more data in RAM.
  */
class ColumnCompressed : public COWHelper<IColumn, ColumnCompressed>
{
public:
    using Lazy = std::function<ColumnPtr()>;

    ColumnCompressed(size_t rows_, size_t bytes_, Lazy lazy_)
        : rows(rows_), bytes(bytes_), lazy(lazy_)
    {
    }

    const char * getFamilyName() const override { return "Compressed"; }

    size_t size() const override { return rows; }
    size_t byteSize() const override { return bytes; }
    size_t allocatedBytes() const override { return bytes; }

    ColumnPtr decompress() const override
    {
        return lazy();
    }

    /** Wrap uncompressed column without compression.
      * Method can be used when compression is not worth doing.
      * But returning CompressedColumn is still needed to keep uniform block structure.
      */
    static ColumnPtr wrap(ColumnPtr column)
    {
        return ColumnCompressed::create(
            column->size(),
            column->allocatedBytes(),
            [column = std::move(column)]{ return column; });
    }

    /// Helper methods for compression.

    /// If data is not worth to be compressed and not 'always_compress' - returns nullptr.
    /// Note: shared_ptr is to allow to be captured by std::function.
    static std::shared_ptr<Memory<>> compressBuffer(const void * data, size_t data_size, bool always_compress);

    static void decompressBuffer(
        const void * compressed_data, void * decompressed_data, size_t compressed_size, size_t decompressed_size);

    /// All other methods throw exception.

    TypeIndex getDataType() const override { throwMustBeDecompressed(); }
    Field operator[](size_t) const override { throwMustBeDecompressed(); }
    void get(size_t, Field &) const override { throwMustBeDecompressed(); }
    StringRef getDataAt(size_t) const override { throwMustBeDecompressed(); }
    void insert(const Field &) override { throwMustBeDecompressed(); }
    void insertRangeFrom(const IColumn &, size_t, size_t) override { throwMustBeDecompressed(); }
    void insertData(const char *, size_t) override { throwMustBeDecompressed(); }
    void insertDefault() override { throwMustBeDecompressed(); }
    void popBack(size_t) override { throwMustBeDecompressed(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwMustBeDecompressed(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwMustBeDecompressed(); }
    const char * skipSerializedInArena(const char *) const override { throwMustBeDecompressed(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwMustBeDecompressed(); }
    void updateWeakHash32(WeakHash32 &) const override { throwMustBeDecompressed(); }
    void updateHashFast(SipHash &) const override { throwMustBeDecompressed(); }
    ColumnPtr filter(const Filter &, ssize_t) const override { throwMustBeDecompressed(); }
    ColumnPtr permute(const Permutation &, size_t) const override { throwMustBeDecompressed(); }
    ColumnPtr index(const IColumn &, size_t) const override { throwMustBeDecompressed(); }
    int compareAt(size_t, size_t, const IColumn &, int) const override { throwMustBeDecompressed(); }
    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
        throwMustBeDecompressed();
    }
    bool hasEqualValues() const override
    {
        throwMustBeDecompressed();
    }
    void getPermutation(bool, size_t, int, Permutation &) const override { throwMustBeDecompressed(); }
    void updatePermutation(bool, size_t, int, Permutation &, EqualRanges &) const override { throwMustBeDecompressed(); }
    ColumnPtr replicate(const Offsets &) const override { throwMustBeDecompressed(); }
    MutableColumns scatter(ColumnIndex, const Selector &) const override { throwMustBeDecompressed(); }
    void gather(ColumnGathererStream &) override { throwMustBeDecompressed(); }
    void getExtremes(Field &, Field &) const override { throwMustBeDecompressed(); }
    size_t byteSizeAt(size_t) const override { throwMustBeDecompressed(); }

protected:
    size_t rows;
    size_t bytes;

    Lazy lazy;

private:
    [[noreturn]] void throwMustBeDecompressed() const
    {
        throw Exception("ColumnCompressed must be decompressed before use", ErrorCodes::LOGICAL_ERROR);
    }
};

}

