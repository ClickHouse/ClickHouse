#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Port.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>

#include <array>
#include <limits>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Writes the rows of one sparse column of a chunk: the value index of each row is computed once, the default value is serialized once.
class SerializationSparseRows final : public SerializationWrapper
{
public:
    SerializationSparseRows(const SerializationPtr & sparse, ColumnPtr column_)
        : SerializationWrapper(sparse)
        , values_serialization(assert_cast<const SerializationSparse &>(*sparse).getNested())
        , column(std::move(column_))
    {
        const auto & column_sparse = assert_cast<const ColumnSparse &>(*column);
        values = &column_sparse.getValuesColumn();
        const auto & offsets = column_sparse.getOffsetsData();
        if (!offsets.empty())
        {
            value_index.resize_fill(column_sparse.size(), 0);
            for (size_t i = 0; i < offsets.size(); ++i)
                value_index[offsets[i]] = static_cast<UInt32>(i + 1);
        }
    }

    void serializeBinary(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeBinary, 0>(col, row_num, ostr, settings);
    }
    void serializeTextEscaped(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeTextEscaped, 1>(col, row_num, ostr, settings);
    }
    void serializeTextQuoted(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeTextQuoted, 2>(col, row_num, ostr, settings);
    }
    void serializeTextCSV(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeTextCSV, 3>(col, row_num, ostr, settings);
    }
    void serializeText(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeText, 4>(col, row_num, ostr, settings);
    }
    void serializeTextJSON(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeTextJSON, 5>(col, row_num, ostr, settings);
    }
    void serializeTextXML(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeTextXML, 6>(col, row_num, ostr, settings);
    }
    void serializeTextRaw(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeRow<&ISerialization::serializeTextRaw, 7>(col, row_num, ostr, settings);
    }

    /// `SerializationSparse` does not override it, so `ISerialization`'s default applies there: `serializeTextJSON`.
    void serializeTextJSONPretty(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t /*indent*/) const override
    {
        serializeTextJSON(col, row_num, ostr, settings);
    }

private:
    using SerializeRow = void (ISerialization::*)(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const;

    template <SerializeRow serialize, size_t slot>
    void serializeRow(const IColumn & col, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
    {
        if (&col != column.get())
        {
            (nested_serialization.get()->*serialize)(col, row_num, ostr, settings);
            return;
        }

        size_t index = value_index.empty() ? 0 : value_index[row_num];
        if (index != 0)
        {
            (values_serialization.get()->*serialize)(*values, index, ostr, settings);
            return;
        }

        if (defaults[slot].settings != &settings) [[unlikely]]
            cacheDefault(serialize, slot, settings);

        const auto & bytes = defaults[slot].bytes;
        /// A `memcpy` call costs more than these few bytes; a closed buffer still goes to `write`.
        if (!ostr.isFinalized() && !ostr.isCanceled() && bytes.size() <= MAX_INLINE_WRITE_SIZE && bytes.size() <= ostr.available())
        {
            memcpySmall(ostr.position(), bytes.data(), bytes.size());
            ostr.position() += bytes.size();
        }
        else
        {
            ostr.write(bytes.data(), bytes.size());
        }
    }

    /// Out of line, so that its write buffer does not put a stack protector canary on every row.
    NO_INLINE void cacheDefault(SerializeRow serialize, size_t slot, const FormatSettings & settings) const
    {
        auto & cached = defaults[slot];
        cached.settings = nullptr;
        cached.bytes.clear();
        WriteBufferFromStringWithMemoryTracking buf(cached.bytes);
        (values_serialization.get()->*serialize)(*values, 0, buf, settings);
        buf.finalize();
        /// Lets a short default use the inline buffer of the string, which is cheaper to read on every row.
        if (cached.bytes.size() <= MAX_INLINE_WRITE_SIZE)
            cached.bytes.shrink_to_fit();
        cached.settings = &settings;
    }

    const SerializationPtr values_serialization;
    /// Holds the column, so that no other column gets its address while this serialization is used.
    const ColumnPtr column;
    const IColumn * values = nullptr;
    /// Index of the value of each row, 0 for the default. Empty if all rows have the default.
    PaddedPODArray<UInt32> value_index;

    struct CachedDefault
    {
        const FormatSettings * settings = nullptr;
        StringWithMemoryTracking bytes;
    };
    mutable std::array<CachedDefault, 8> defaults;
};

}

IRowOutputFormat::IRowOutputFormat(SharedHeader header, WriteBuffer & out_)
    : IOutputFormat(header, out_)
    , num_columns(header->columns())
    , types(header->getDataTypes())
    , serializations(header->getSerializations())
{
}

void IRowOutputFormat::consume(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    updateSerializationsIfNeeded(columns);

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (haveWrittenData())
            writeRowBetweenDelimiter();

        write(columns, row);
        first_row = false;
    }
}

void IRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    if (!supportTotals())
        return;

    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in totals chunk, expected 1", num_rows);

    const auto & columns = chunk.getColumns();
    updateSerializationsIfNeeded(columns);

    writeBeforeTotals();
    writeTotals(columns, 0);
    writeAfterTotals();
}

void IRowOutputFormat::consumeExtremes(DB::Chunk chunk)
{
    if (!supportExtremes())
        return;

    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    updateSerializationsIfNeeded(columns);

    if (num_rows != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in extremes chunk, expected 2", num_rows);

    writeBeforeExtremes();
    writeMinExtreme(columns, 0);
    writeRowBetweenDelimiter();
    writeMaxExtreme(columns, 1);
    writeAfterExtremes();
}

void IRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    writeRowStartDelimiter();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeRowEndDelimiter();
}

void IRowOutputFormat::writeMinExtreme(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::writeMaxExtreme(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::writeTotals(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::updateSerializationsIfNeeded(const Columns & columns)
{
    if (supportsSpecialSerializationKinds())
    {
        for (size_t i = 0; i != columns.size(); ++i)
        {
            serializations[i] = types[i]->getSerialization(*types[i]->getSerializationInfo(*columns[i]));
            if (typeid_cast<const SerializationSparse *>(serializations[i].get()) && typeid_cast<const ColumnSparse *>(columns[i].get())
                && columns[i]->size() <= std::numeric_limits<UInt32>::max())
                serializations[i] = std::make_shared<SerializationSparseRows>(serializations[i], columns[i]);
        }
    }
}

}
