#pragma once
#include <memory>
#pragma GCC diagnostic ignored "-Wcast-align"
#pragma GCC diagnostic ignored "-Wcast-qual"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"

#include <optional>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <Columns/IColumn.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

// #include <fsst.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ColumnFSST : public COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>
{
public:
    struct Header {

    };
    using Char = UInt8;
    using Chars = PaddedPODArray<UInt8>;

    class Compressor {
    public:
        virtual void prepare(ReadBuffer & istr) = 0;
        virtual String encode(const String& str) const = 0;

        virtual ~Compressor() = default;
    };

private:

    friend class COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>;

    ColumnFSST(const ColumnFSST & src)
    : COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>(src),
    offsets(src.offsets.begin(), src.offsets.end()),
    chars(src.chars.begin(), src.chars.end())
    {
    }

    ColumnFSST() = default;

public:

    const char * getFamilyName() const override { return "FSST"; }

    size_t size() const override { return offsets.size(); }
    size_t byteSize() const override { return chars.size(); }
    size_t allocatedBytes() const override { return chars.size(); }

    ColumnPtr decompress() const override
    {
        throwMustBeDecompressed();
    }

    /// All other methods throw exception.

    TypeIndex getDataType() const override { throwMustBeDecompressed(); }
    Field operator[](size_t) const override { throwMustBeDecompressed(); }
    void get(size_t, Field &) const override { throwMustBeDecompressed(); }
    StringRef getDataAt(size_t) const override { throwMustBeDecompressed(); }
    bool isDefaultAt(size_t) const override { throwMustBeDecompressed(); }
    void insert(const Field &) override { throwMustBeDecompressed(); }
    bool tryInsert(const Field &) override { throwMustBeDecompressed(); }
    void insertRangeFrom(const IColumn &, size_t, size_t) override { throwMustBeDecompressed(); }
    void insertData(const char *, size_t) override { throwMustBeDecompressed(); }
    void insertDefault() override { throwMustBeDecompressed(); }
    void popBack(size_t) override { throwMustBeDecompressed(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwMustBeDecompressed(); }
    char * serializeValueIntoMemory(size_t, char *) const override { throwMustBeDecompressed(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwMustBeDecompressed(); }
    const char * skipSerializedInArena(const char *) const override { throwMustBeDecompressed(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwMustBeDecompressed(); }
    void updateWeakHash32(WeakHash32 &) const override { throwMustBeDecompressed(); }
    void updateHashFast(SipHash &) const override { throwMustBeDecompressed(); }
    ColumnPtr filter(const Filter &, ssize_t) const override { throwMustBeDecompressed(); }
    void expand(const Filter &, bool) override { throwMustBeDecompressed(); }
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
    void getPermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                        size_t, int, Permutation &) const override { throwMustBeDecompressed(); }
    void updatePermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                        size_t, int, Permutation &, EqualRanges &) const override { throwMustBeDecompressed(); }
    ColumnPtr replicate(const Offsets &) const override { throwMustBeDecompressed(); }
    MutableColumns scatter(ColumnIndex, const Selector &) const override { throwMustBeDecompressed(); }
    void gather(ColumnGathererStream &) override { throwMustBeDecompressed(); }
    void getExtremes(Field &, Field &) const override { throwMustBeDecompressed(); }
    size_t byteSizeAt(size_t) const override { throwMustBeDecompressed(); }
    double getRatioOfDefaultRows(double) const override { throwMustBeDecompressed(); }
    UInt64 getNumberOfDefaultRows() const override { throwMustBeDecompressed(); }
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override { throwMustBeDecompressed(); }


    Chars & getChars() { return chars; }
    const Chars & getChars() const { return chars; }

    Offsets & getOffsets() { return offsets; }
    const Offsets & getOffsets() const { return offsets; }

    void doPrepare(ReadBuffer & istr) const;
    String encode(const String& str) const { return compressor->encode(str); }

protected:
    mutable Offsets offsets;
    mutable Chars chars;

    mutable std::shared_ptr<Compressor> compressor;

private:
    [[noreturn]] static void throwMustBeDecompressed()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnCompressed must be decompressed before use");
    }
};

}
