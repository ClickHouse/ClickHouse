#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockOutputFormat.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Port.h>
#include <Core/Block.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/assert_cast.h>
#include <IO/WriteBuffer.h>
#include <IO/NetUtils.h>

namespace DB
{

namespace
{
constexpr std::string_view ARROW_MAGIC = "ARROW1";

/// Builds the integer index column (and its type) for a dictionary-encoded field from global indices.
std::pair<DataTypePtr, MutableColumnPtr> makeIndexColumn(
    const PaddedPODArray<Int64> & indexes, const ArrowIPC::OutputDictionary & dict)
{
    auto fill = [&](auto * column)
    {
        using ColumnType = std::decay_t<decltype(*column)>;
        using ValueType = typename ColumnType::ValueType;
        auto & data = column->getData();
        data.resize(indexes.size());
        for (size_t i = 0; i < indexes.size(); ++i)
            data[i] = static_cast<ValueType>(indexes[i]);
    };

    if (dict.index_bit_width == 64)
    {
        if (dict.index_is_signed)
        {
            auto col = ColumnInt64::create();
            fill(col.get());
            return {std::make_shared<DataTypeInt64>(), std::move(col)};
        }
        auto col = ColumnUInt64::create();
        fill(col.get());
        return {std::make_shared<DataTypeUInt64>(), std::move(col)};
    }
    if (dict.index_is_signed)
    {
        auto col = ColumnInt32::create();
        fill(col.get());
        return {std::make_shared<DataTypeInt32>(), std::move(col)};
    }
    auto col = ColumnUInt32::create();
    fill(col.get());
    return {std::make_shared<DataTypeUInt32>(), std::move(col)};
}

template <typename T>
void mapIndexesToGlobal(
    const PaddedPODArray<T> & local_indexes, const PaddedPODArray<Int64> & local_to_global,
    PaddedPODArray<Int64> & out, PaddedPODArray<UInt8> * out_null_map)
{
    for (size_t row = 0; row < local_indexes.size(); ++row)
    {
        const Int64 global = local_to_global[local_indexes[row]];
        out[row] = global < 0 ? 0 : global;
        if (out_null_map)
            (*out_null_map)[row] = global < 0 ? 1 : 0;
    }
}

/// Translates a LowCardinality column's per-batch local dictionary indexes into accumulated global ones.
void mapIndexesToGlobal(
    const IColumn & local_indexes, const PaddedPODArray<Int64> & local_to_global,
    PaddedPODArray<Int64> & out, PaddedPODArray<UInt8> * out_null_map)
{
    switch (local_indexes.getDataType())
    {
        case TypeIndex::UInt8:
            mapIndexesToGlobal(assert_cast<const ColumnUInt8 &>(local_indexes).getData(), local_to_global, out, out_null_map); break;
        case TypeIndex::UInt16:
            mapIndexesToGlobal(assert_cast<const ColumnUInt16 &>(local_indexes).getData(), local_to_global, out, out_null_map); break;
        case TypeIndex::UInt32:
            mapIndexesToGlobal(assert_cast<const ColumnUInt32 &>(local_indexes).getData(), local_to_global, out, out_null_map); break;
        default:
            mapIndexesToGlobal(assert_cast<const ColumnUInt64 &>(local_indexes).getData(), local_to_global, out, out_null_map); break;
    }
}
}

ArrowIPCBlockOutputFormat::ArrowIPCBlockOutputFormat(
    WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), stream(stream_), format_settings(format_settings_)
{
    const Block & header = *header_;
    column_names.reserve(header.columns());
    column_types.reserve(header.columns());
    for (const auto & column : header)
    {
        column_names.push_back(column.name);
        column_types.push_back(column.type);
    }
    message_writer.emplace(out);
    encoder = std::make_unique<ArrowIPC::RecordBatchEncoder>(format_settings);

    column_dictionaries = ArrowIPC::assignOutputDictionaries(column_types, format_settings);
    size_t num_dictionaries = 0;
    for (const auto & dict : column_dictionaries)
        if (dict)
            num_dictionaries = std::max(num_dictionaries, static_cast<size_t>(dict->id) + 1);
    dictionary_states.resize(num_dictionaries);
}

void ArrowIPCBlockOutputFormat::writeSchemaIfNeeded()
{
    if (schema_written)
        return;

    /// The file format begins with "ARROW1" + 2 padding bytes so the first message body stays aligned.
    if (!stream)
        message_writer->writeRaw("ARROW1\0\0", ARROW_MAGIC.size() + 2);

    flatbuffers::FlatBufferBuilder builder;
    ArrowIPC::buildSchemaMessage(builder, column_names, column_types, format_settings);
    message_writer->writeMessage(builder.GetBufferPointer(), builder.GetSize(), nullptr, 0);
    schema_written = true;
}

ArrowIPC::MessageWriter::WrittenMessage ArrowIPCBlockOutputFormat::writeBatchMessage(
    const ArrowIPC::RecordBatchEncoder::EncodedBatch & batch, std::optional<int64_t> dictionary_id, bool is_delta)
{
    flatbuffers::FlatBufferBuilder builder;
    flatbuffers::Offset<ArrowIPC::flatbuf::BodyCompression> compression_off = 0;
    if (batch.codec)
    {
        const auto codec_type = *batch.codec == ArrowIPC::CompressionCodec::Zstd
            ? ArrowIPC::flatbuf::CompressionType_ZSTD
            : ArrowIPC::flatbuf::CompressionType_LZ4_FRAME;
        compression_off = ArrowIPC::flatbuf::CreateBodyCompression(
            builder, codec_type, ArrowIPC::flatbuf::BodyCompressionMethod_BUFFER);
    }
    auto nodes_vec = builder.CreateVectorOfStructs(batch.nodes.data(), batch.nodes.size());
    auto buffers_vec = builder.CreateVectorOfStructs(batch.buffers.data(), batch.buffers.size());
    auto record_batch = ArrowIPC::flatbuf::CreateRecordBatch(builder, batch.num_rows, nodes_vec, buffers_vec, compression_off);

    flatbuffers::Offset<void> header_off;
    ArrowIPC::flatbuf::MessageHeader header_type = ArrowIPC::flatbuf::MessageHeader_NONE;
    if (dictionary_id)
    {
        header_type = ArrowIPC::flatbuf::MessageHeader_DictionaryBatch;
        header_off = ArrowIPC::flatbuf::CreateDictionaryBatch(builder, *dictionary_id, record_batch, is_delta).Union();
    }
    else
    {
        header_type = ArrowIPC::flatbuf::MessageHeader_RecordBatch;
        header_off = record_batch.Union();
    }

    auto message = ArrowIPC::flatbuf::CreateMessage(
        builder, ArrowIPC::flatbuf::MetadataVersion_V5, header_type, header_off,
        static_cast<int64_t>(batch.body.size()));
    builder.Finish(message);

    return message_writer->writeMessage(
        builder.GetBufferPointer(), builder.GetSize(), batch.body.data(), batch.body.size());
}

void ArrowIPCBlockOutputFormat::consume(Chunk chunk)
{
    writeSchemaIfNeeded();

    const size_t num_rows = chunk.getNumRows();
    const Columns & columns = chunk.getColumns();

    /// Dictionary-encoded columns are replaced in the record batch by their integer index column; their
    /// values go into separate DictionaryBatch messages (extended across batches via deltas).
    Columns record_columns(columns.begin(), columns.end());
    DataTypes record_types = column_types;

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!column_dictionaries[i])
            continue;
        const ArrowIPC::OutputDictionary & dict = *column_dictionaries[i];
        auto & state = dictionary_states[dict.id];

        const DataTypePtr value_type = removeLowCardinality(column_types[i]);
        const bool value_nullable = value_type->isNullable();
        if (!state.values)
            state.values = value_type->createColumn();

        /// Deduplicate at the dictionary level (a few values), not per row: map each entry of this
        /// batch's LowCardinality dictionary to a global index, extending the accumulated dictionary
        /// with any new values (emitted as a delta). The null placeholder maps to -1 (marked via the
        /// index validity bitmap, never referenced from the Arrow dictionary).
        const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(*columns[i]);
        const ColumnPtr & batch_dictionary = low_cardinality.getDictionary().getNestedColumn();
        const size_t dict_size = batch_dictionary->size();

        const size_t delta_start = state.values->size();
        PaddedPODArray<Int64> local_to_global(dict_size);
        for (size_t e = 0; e < dict_size; ++e)
        {
            if (value_nullable && batch_dictionary->isNullAt(e))
            {
                local_to_global[e] = -1;
                continue;
            }
            const std::string key(batch_dictionary->getDataAt(e));
            auto [it, inserted] = state.value_to_index.try_emplace(key, static_cast<Int64>(state.values->size()));
            if (inserted)
                state.values->insertFrom(*batch_dictionary, e);
            local_to_global[e] = it->second;
        }

        PaddedPODArray<Int64> indexes(num_rows);
        auto index_null_map = value_nullable ? ColumnUInt8::create() : nullptr;
        if (index_null_map)
            index_null_map->getData().resize(num_rows);
        mapIndexesToGlobal(
            low_cardinality.getIndexes(), local_to_global, indexes,
            index_null_map ? &index_null_map->getData() : nullptr);

        /// Emit the new dictionary values (a delta, except the first batch which registers the id).
        const size_t delta_size = state.values->size() - delta_start;
        if (!state.emitted || delta_size > 0)
        {
            ColumnPtr delta = state.values->cut(delta_start, delta_size);
            auto dict_batch = encoder->encode({delta}, {value_type}, delta_size);
            auto written = writeBatchMessage(dict_batch, dict.id, /*is_delta=*/state.emitted);
            if (!stream)
                dictionary_blocks.push_back({written.offset, written.metadata_length, written.body_length});
            state.emitted = true;
        }

        auto [index_type, index_column] = makeIndexColumn(indexes, dict);
        if (value_nullable)
        {
            record_columns[i] = ColumnNullable::create(std::move(index_column), std::move(index_null_map));
            record_types[i] = std::make_shared<DataTypeNullable>(index_type);
        }
        else
        {
            record_columns[i] = std::move(index_column);
            record_types[i] = index_type;
        }
    }

    auto batch = encoder->encode(record_columns, record_types, num_rows);
    auto written = writeBatchMessage(batch);
    if (!stream)
        record_blocks.push_back({written.offset, written.metadata_length, written.body_length});
}

void ArrowIPCBlockOutputFormat::finalizeImpl()
{
    /// Make sure even an empty result produces a valid stream/file (schema, then EOS or footer).
    writeSchemaIfNeeded();

    if (stream)
    {
        message_writer->writeEOS();
        return;
    }

    /// File format trailer: <footer FlatBuffer> <int32 footer length LE> <"ARROW1">.
    flatbuffers::FlatBufferBuilder builder;
    ArrowIPC::buildFooter(builder, column_names, column_types, format_settings, dictionary_blocks, record_blocks);
    out.write(reinterpret_cast<const char *>(builder.GetBufferPointer()), builder.GetSize());

    int32_t footer_length = DB::toLittleEndian(static_cast<int32_t>(builder.GetSize()));
    out.write(reinterpret_cast<const char *>(&footer_length), sizeof(footer_length));
    out.write(ARROW_MAGIC.data(), ARROW_MAGIC.size());
}

void ArrowIPCBlockOutputFormat::resetFormatterImpl()
{
    message_writer.emplace(out);
    schema_written = false;
    dictionary_blocks.clear();
    record_blocks.clear();
    for (auto & state : dictionary_states)
        state = DictionaryColumnState{};
}

}

#endif
