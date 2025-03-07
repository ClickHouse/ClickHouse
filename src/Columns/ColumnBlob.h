#pragma once

#include <Columns/IColumn.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Field.h>
#include <Formats/NativeWriter.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/WeakHash.h>
#include "Compression/CompressedReadBuffer.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "Formats/NativeReader.h"
#include "base/defines.h"

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


class ColumnBlob : public COWHelper<IColumnHelper<ColumnBlob>, ColumnBlob>
{
public:
    using Blob = std::vector<char>;

    // The argument is supposed to be a some ColumnBlob's internal blob.
    using ToBlob = std::function<void(Blob &)>;

    // The argument is supposed to be a some ColumnBlob's internal blob,
    // the return value is the reconstructed column.
    // TODO(nickitat): fix signature
    using FromBlob = std::function<ColumnPtr(const Blob &, int)>;

    ColumnBlob(ToBlob task, ColumnPtr concrete_column_)
        : rows(concrete_column_->size()), concrete_column(std::move(concrete_column_)), to_blob_task(std::move(task))
    {
    }

    ColumnBlob(FromBlob task, size_t rows_) : rows(rows_), from_blob_task(std::move(task)) { }

    const char * getFamilyName() const override { return "Blob"; }

    size_t size() const override { return rows; }

    bool concreteIsSparse() const
    {
        chassert(concrete_column);
        return concrete_column->isSparse();
    }

    MutableColumnPtr cloneEmpty() const override
    {
        chassert(concrete_column);
        return concrete_column->cloneEmpty();
    }

    Blob & getBlob() { return blob; }
    const Blob & getBlob() const { return blob; }

    void convertTo()
    {
        chassert(to_blob_task);
        to_blob_task(blob);
    }

    ColumnPtr convertFrom() const
    {
        chassert(from_blob_task);
        return from_blob_task(blob, 0);
    }

    /// Creates serialized and compressed blob from the source column.
    static void toBlob(
        Blob & blob,
        ColumnWithTypeAndName concrete_column,
        CompressionCodecPtr codec,
        UInt64 client_revision,
        const std::optional<FormatSettings> & format_settings)
    {
        WriteBufferFromVector<Blob> wbuf(blob);
        // TODO(nickitat): avoid double compression
        CompressedWriteBuffer compressed_buffer(wbuf, codec);
        auto serialization = NativeWriter::getSerialization(client_revision, concrete_column);
        NativeWriter::writeData(
            *serialization, concrete_column.column, compressed_buffer, format_settings, 0, concrete_column.column->size(), client_revision);
        compressed_buffer.finalize();
    }

    /// Decompresses and deserializes the blob into the source column.
    static ColumnPtr fromBlob(
        const Blob & blob, ColumnPtr nested, SerializationPtr nested_serialization, size_t rows, const FormatSettings * format_settings)
    {
        ReadBufferFromMemory rbuf(blob.data(), blob.size());
        CompressedReadBuffer decompressed_buffer(rbuf);
        // TODO(nickitat): support
        double avg_value_size_hint = 0;
        NativeReader::readData(*nested_serialization, nested, decompressed_buffer, format_settings, rows, avg_value_size_hint);
        return nested;
    }

    /// All other methods throw the exception.

    // TODO(nickitat): implement
    size_t byteSize() const override { return blob.size(); }
    size_t allocatedBytes() const override { return blob.size(); }

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
    Blob blob;

    const size_t rows;
    ColumnPtr concrete_column;
    ToBlob to_blob_task;
    FromBlob from_blob_task;

    [[noreturn]] static void throwInapplicable()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBlob should be converted to a regular column before usage");
    }
};

inline Block convertBlobColumns(const Block & block)
{
    Block res;
    res.info = block.info;
    for (const auto & elem : block)
    {
        ColumnWithTypeAndName column = elem;
        if (const auto * col = typeid_cast<const ColumnBlob *>(column.column.get()))
            column.column = col->convertFrom();
        res.insert(std::move(column));
    }
    return res;
}
}
