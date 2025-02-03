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
#include "Formats/NativeReader.h"


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

    // TODO(nickitat): fix me
    explicit ColumnBlob(ColumnPtr src_col_) { src_col.column = std::move(src_col_); }

    ColumnBlob(
        ColumnWithTypeAndName src_col_, CompressionCodecPtr codec_, UInt64 client_revision_, std::optional<FormatSettings> format_settings_)
        : src_col(std::move(src_col_))
        , codec(std::move(codec_))
        , client_revision(client_revision_)
        , format_settings(std::move(format_settings_))
    {
        preprocess();
    }

    const char * getFamilyName() const override { return "Blob"; }

    size_t size() const override { return src_col.column->size(); }
    size_t byteSize() const override { return src_col.column->byteSize(); }
    size_t allocatedBytes() const override { return src_col.column->allocatedBytes(); }

    const ColumnPtr & getNestedColumn() const { return src_col.column; }

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
    using Blob = std::vector<std::byte>;

    Blob blob;

    // Maybe we shouldn't keep this reference
    ColumnWithTypeAndName src_col;

    CompressionCodecPtr codec;
    UInt64 client_revision;
    std::optional<FormatSettings> format_settings;

    // void ISerialization::serializeBinaryBulk(const IColumn & column, WriteBuffer &, size_t, size_t) const
    // {
    //     throw Exception(ErrorCodes::MULTIPLE_STREAMS_REQUIRED, "Column {} must be serialized with multiple streams", column.getName());
    // }
    //
    // void ISerialization::deserializeBinaryBulk(IColumn & column, ReadBuffer &, size_t, double) const
    // {
    //     throw Exception(ErrorCodes::MULTIPLE_STREAMS_REQUIRED, "Column {} must be deserialized with multiple streams", column.getName());
    // }

    /// Creates serialized and compressed blob from the source column.
    void preprocess()
    {
        WriteBufferFromVector<Blob> wbuf(blob);
        CompressedWriteBuffer compressed_buffer(wbuf, codec);
        auto serialization = NativeWriter::getSerialization(client_revision, src_col);
        NativeWriter::writeData(
            *serialization, src_col.column, compressed_buffer, format_settings, 0, src_col.column->size(), client_revision);
        compressed_buffer.finalize();
    }

    /// Decompresses and deserializes blob into the source column.
    void postprocess()
    {
        ReadBufferFromMemory rbuf(blob.data(), blob.size());
        CompressedReadBuffer decompressed_buffer(rbuf);
        auto serialization = NativeWriter::getSerialization(client_revision, src_col);
        // TODO(nickitat): support
        size_t rows = 0;
        double avg_value_size_hint = 0;
        NativeReader::readData(*serialization, src_col.column, decompressed_buffer, format_settings, rows, avg_value_size_hint);
    }

    [[noreturn]] static void throwInapplicable()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBlob should be converted to a regular column before usage");
    }
};

}
