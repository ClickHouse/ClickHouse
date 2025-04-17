#pragma once

#include <Columns/IColumn.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/castColumn.h>
#include <base/defines.h>
#include <Common/PODArray.h>
#include <Common/WeakHash.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class ColumnBLOB : public COWHelper<IColumnHelper<ColumnBLOB>, ColumnBLOB>
{
public:
    using BLOB = PODArray<char>;

    // The argument is supposed to be some ColumnBLOB's internal BLOB,
    // the return value is the reconstructed column.
    using FromBLOB = std::function<ColumnPtr(const BLOB &)>;

    ColumnBLOB(
        ColumnWithTypeAndName wrapped_column_, CompressionCodecPtr codec, UInt64 client_revision, const FormatSettings & format_settings)
        : rows(wrapped_column_.column->size())
        , wrapped_column(wrapped_column_.column)
    {
        chassert(wrapped_column);
        toBLOB(blob, wrapped_column_, codec, client_revision, format_settings);
    }

    ColumnBLOB(FromBLOB task, ColumnPtr wrapped_column_, size_t rows_)
        : rows(rows_)
        , wrapped_column(std::move(wrapped_column_))
        , from_blob_task(std::move(task))
    {
        chassert(wrapped_column);
    }

    ColumnBLOB(const ColumnBLOB & other)
        : COWHelper(other)
        , blob(other.blob.begin(), other.blob.end())
        , rows(other.rows)
        , wrapped_column(other.wrapped_column)
        , from_blob_task(other.from_blob_task)
        , cast_from(other.cast_from)
        , cast_to(other.cast_to)
    {
        chassert(wrapped_column);
    }

    const char * getFamilyName() const override { return "BLOB"; }

    size_t size() const override { return rows; }

    bool wrappedColumnIsSparse() const
    {
        chassert(wrapped_column);
        return wrapped_column->isSparse();
    }

    ColumnPtr getWrappedColumn() const
    {
        chassert(wrapped_column);
        return wrapped_column;
    }

    MutableColumnPtr cloneEmpty() const override
    {
        chassert(wrapped_column);
        return wrapped_column->cloneEmpty();
    }

    BLOB & getBLOB() { return blob; }

    const BLOB & getBLOB() const { return blob; }

    ColumnPtr convertFrom() const
    {
        chassert(from_blob_task);
        ColumnWithTypeAndName col;
        col.column = from_blob_task(blob);
        col.type = cast_from;
        return cast_to ? DB::castColumn(col, cast_to) : col.column;
    }

    /// Creates serialized and compressed blob from the source column.
    static void toBLOB(
        BLOB & blob,
        ColumnWithTypeAndName wrapped_column,
        CompressionCodecPtr codec,
        UInt64 client_revision,
        const std::optional<FormatSettings> & format_settings)
    {
        WriteBufferFromVector<BLOB> wbuf(blob);
        CompressedWriteBuffer compressed_buffer(wbuf, codec);
        auto serialization = NativeWriter::getSerialization(client_revision, wrapped_column);
        NativeWriter::writeData(
            *serialization, wrapped_column.column, compressed_buffer, format_settings, 0, wrapped_column.column->size(), client_revision);
        compressed_buffer.finalize();
    }

    /// Decompresses and deserializes the blob into the source column.
    static ColumnPtr fromBLOB(
        const BLOB & blob,
        ColumnPtr nested,
        SerializationPtr nested_serialization,
        size_t rows,
        const FormatSettings * format_settings,
        double avg_value_size_hint)
    {
        ReadBufferFromMemory rbuf(blob.data(), blob.size());
        CompressedReadBuffer decompressed_buffer(rbuf);
        chassert(nested->empty());
        NativeReader::readData(*nested_serialization, nested, decompressed_buffer, format_settings, rows, avg_value_size_hint);
        return nested;
    }

    void addCast(const DataTypePtr & from, const DataTypePtr & to) const
    {
        cast_from = from;
        cast_to = to;
    }

    size_t byteSize() const override { return blob.size(); }

    size_t allocatedBytes() const override { return blob.capacity(); }

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
    BLOB blob;
    const size_t rows;
    ColumnPtr wrapped_column;
    FromBLOB from_blob_task;

    mutable DataTypePtr cast_from;
    mutable DataTypePtr cast_to;

    [[noreturn]] void throwInapplicable() const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBLOB should be converted to a regular column before usage");
    }
};

[[nodiscard]] inline Block convertBLOBColumns(const Block & block)
{
    Block res;
    res.info = block.info;
    for (const auto & elem : block)
    {
        ColumnWithTypeAndName column = elem;
        if (const auto * col = typeid_cast<const ColumnBLOB *>(column.column.get()))
            column.column = col->convertFrom();
        res.insert(std::move(column));
    }
    return res;
}
}
