#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>

#if USE_ARROW

#include <Processors/Port.h>
#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <Common/WKB.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileSize.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/NestedUtils.h>
#include <Core/UUID.h>
#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <Common/assert_cast.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int THERE_IS_NO_COLUMN;
    extern const int DUPLICATE_COLUMN;
}

ArrowIPCBlockInputFormat::ArrowIPCBlockInputFormat(
    ReadBuffer & in_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IInputFormat(header_, &in_)
    , stream(stream_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

ArrowIPCBlockInputFormat::~ArrowIPCBlockInputFormat() = default;

void ArrowIPCBlockInputFormat::collectDictionaryFields(const std::vector<ArrowIPC::ArrowField> & fields)
{
    for (const auto & field : fields)
    {
        if (field.dictionary)
        {
            /// The dictionary batch carries the plain value column: same type, but not dictionary-encoded.
            ArrowIPC::ArrowField value_field = field;
            value_field.dictionary.reset();
            dictionary_value_fields[field.dictionary->id] = std::move(value_field);
        }
        collectDictionaryFields(field.type.children);
    }
}

namespace
{
/// A LowCardinality dictionary must have unique values; the library reader rejects non-unique ones.
/// Validate the common (non-nullable, contiguously-comparable) dictionary value columns.
void checkDictionaryUnique(const ColumnPtr & values)
{
    const IColumn * inner = values.get();
    const ColumnNullable * nullable = nullptr;
    if (values->isNullable())
    {
        nullable = &assert_cast<const ColumnNullable &>(*values);
        inner = &nullable->getNestedColumn();
    }
    /// Only validate value columns that can be compared cheaply and contiguously.
    if (!(inner->isFixedAndContiguous() || typeid_cast<const ColumnString *>(inner)))
        return;

    std::unordered_set<std::string_view> seen;
    seen.reserve(values->size());
    bool null_seen = false;
    for (size_t i = 0; i < values->size(); ++i)
    {
        if (nullable && nullable->getNullMapData()[i])
        {
            if (null_seen)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow dictionary contains duplicate values");
            null_seen = true;
        }
        else if (!seen.emplace(inner->getDataAt(i)).second)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow dictionary contains duplicate values");
        }
    }
}
}

void ArrowIPCBlockInputFormat::prepareReader()
{
    if (stream)
        prepareStreamReader();
    else
        prepareFileReader();

    /// GeoParquet geometry columns (schema-level "geo" metadata) are decoded from WKB/WKT into geo types.
    if (format_settings.parquet.allow_geoparquet_parser)
    {
        auto geo_it = arrow_schema->custom_metadata.find("geo");
        geo_columns = parseGeoMetadataEncoding(
            geo_it != arrow_schema->custom_metadata.end() ? &geo_it->second : nullptr);
    }

    /// Reject duplicate column names, matching the Apache Arrow library based reader.
    std::unordered_set<String> seen_names;
    for (const auto & field : arrow_schema->fields)
        if (!seen_names.insert(field.name).second)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Duplicate column '{}' in the Arrow schema", field.name);

    collectDictionaryFields(arrow_schema->fields);
    decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings, dictionaries);
    prepared = true;
}

void ArrowIPCBlockInputFormat::prepareStreamReader()
{
    message_reader.emplace(*in);

    ArrowIPC::MessageReader::Message msg;
    if (!message_reader->readNextMessage(msg))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");
    if (msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_Schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The first Arrow IPC message must be the schema");

    arrow_schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema());
    /// The schema message has no body, but be defensive in case a producer emits a (zero-length) one.
    message_reader->skipBody(msg.body_length);
}

void ArrowIPCBlockInputFormat::prepareFileReader()
{
    /// The file format requires random access. Use the input directly when it is seekable, otherwise
    /// load it entirely into memory (matching the Apache Arrow library's behaviour for such inputs).
    /// The file format needs random access. Use the input directly only when it is genuinely a
    /// seekable file with a known size (`tryGetFileSizeFromReadBuffer` returns 0 for pipes); otherwise
    /// load it entirely into memory, matching the Apache Arrow library's behaviour for such inputs.
    size_t file_size = 0;
    seekable = dynamic_cast<SeekableReadBuffer *>(in);
    std::optional<size_t> known_size;
    if (seekable)
        known_size = tryGetFileSizeFromReadBuffer(*in);
    if (seekable && known_size && *known_size > 0)
    {
        file_size = *known_size;
    }
    else
    {
        readStringUntilEOF(file_data, *in);
        file_size = file_data.size();
        memory_buffer = std::make_unique<ReadBufferFromMemory>(file_data.data(), file_data.size());
        seekable = assert_cast<SeekableReadBuffer *>(memory_buffer.get());
    }
    message_reader.emplace(*seekable);

    ArrowIPC::ArrowFileFooter footer = ArrowIPC::readArrowFileFooter(*seekable, file_size);
    arrow_schema = std::move(footer.schema);
    for (const auto & block : footer.record_batch_blocks)
        record_batch_blocks.push_back({block.offset, block.body_length});

    /// Decode all dictionary batches up front (the registry must be populated before any record batch).
    collectDictionaryFields(arrow_schema->fields);
    auto temp_decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings, dictionaries);
    for (const auto & block : footer.dictionary_blocks)
    {
        seekable->seek(block.offset, SEEK_SET);
        ArrowIPC::MessageReader::Message msg;
        if (!message_reader->readNextMessage(msg) || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_DictionaryBatch)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a dictionary batch in the Arrow file");

        const auto * dict_batch = msg.header->header_as_DictionaryBatch();
        const int64_t id = dict_batch->id();
        auto field_it = dictionary_value_fields.find(id);
        if (field_it == dictionary_value_fields.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow file dictionary batch for unknown id {}", id);

        message_reader->readBody(msg.body_length, body_buffer);
        auto decoded = temp_decoder->decodeColumns(*dict_batch->data(), body_buffer, {field_it->second});
        checkDictionaryUnique(decoded.at(0).column);
        dictionaries.set(id, decoded.at(0).column, dict_batch->isDelta());
    }
}

void ArrowIPCBlockInputFormat::reinterpretFixedSizeBinary(ColumnWithTypeAndName & column, const DataTypePtr & to_type)
{
    const DataTypePtr target_no_null = removeNullable(to_type);
    const WhichDataType target(target_no_null);
    const ColumnPtr & src = column.column;

    /// A fixed_size_binary(16) read into a UUID column (e.g. an external file without the arrow.uuid
    /// extension): reinterpret the 16 bytes with the same half-reversal the library import uses.
    if (target.isUUID())
    {
        const ColumnPtr nested = src->isNullable()
            ? assert_cast<const ColumnNullable &>(*src).getNestedColumnPtr() : src;
        if (const auto * fixed = typeid_cast<const ColumnFixedString *>(nested.get()); fixed && fixed->getN() == 16)
        {
            const size_t rows = fixed->size();
            auto uuids = ColumnVector<UUID>::create(rows);
            for (size_t i = 0; i < rows; ++i)
            {
                auto * dst = reinterpret_cast<uint8_t *>(&uuids->getData()[i]);
                memcpy(dst, &fixed->getChars()[i * 16], 16);
                std::reverse(dst, dst + 8);
                std::reverse(dst + 8, dst + 16);
            }
            ColumnPtr result = std::move(uuids);
            DataTypePtr result_type = target_no_null;
            if (src->isNullable())
            {
                result = ColumnNullable::create(result, assert_cast<const ColumnNullable &>(*src).getNullMapColumnPtr());
                result_type = std::make_shared<DataTypeNullable>(target_no_null);
            }
            column.column = std::move(result);
            column.type = std::move(result_type);
        }
        return;
    }

    /// Big integers are decoded as FixedString (their Arrow fixed_size_binary representation); castColumn
    /// would try to parse them as text, so reinterpret the raw little-endian bytes instead.
    const bool target_is_big_int = target.isInt128() || target.isUInt128() || target.isInt256() || target.isUInt256();
    if (target_is_big_int)
    {
        ColumnPtr null_map;
        const ColumnPtr nested = src->isNullable()
            ? assert_cast<const ColumnNullable &>(*src).getNestedColumnPtr()
            : src;
        if (src->isNullable())
            null_map = assert_cast<const ColumnNullable &>(*src).getNullMapColumnPtr();

        if (const auto * fixed = typeid_cast<const ColumnFixedString *>(nested.get()))
        {
            const size_t width = target.isInt256() || target.isUInt256() ? 32 : 16;
            if (fixed->getN() == width)
            {
                const size_t rows = fixed->size();
                auto ints = target_no_null->createColumn();
                auto copy = [&](auto & data) { data.resize(rows); if (rows) memcpy(data.data(), fixed->getChars().data(), rows * width); };
                switch (target.idx)
                {
                    case TypeIndex::Int128: copy(assert_cast<ColumnVector<Int128> &>(*ints).getData()); break;
                    case TypeIndex::UInt128: copy(assert_cast<ColumnVector<UInt128> &>(*ints).getData()); break;
                    case TypeIndex::Int256: copy(assert_cast<ColumnVector<Int256> &>(*ints).getData()); break;
                    default: copy(assert_cast<ColumnVector<UInt256> &>(*ints).getData()); break;
                }
                ColumnPtr result = std::move(ints);
                DataTypePtr result_type = target_no_null;
                if (null_map)
                {
                    result = ColumnNullable::create(result, null_map);
                    result_type = std::make_shared<DataTypeNullable>(target_no_null);
                }
                column.column = std::move(result);
                column.type = std::move(result_type);
            }
        }
    }
}

ColumnPtr ArrowIPCBlockInputFormat::decodeGeoColumn(const ColumnPtr & source, const GeoColumnMetadata & geo_metadata)
{
    DataTypePtr type = getGeoDataType(geo_metadata.type);
    MutableColumnPtr column = type->createColumn();

    const IColumn * data = source.get();
    const ColumnNullable * nullable = nullptr;
    if (source->isNullable())
    {
        nullable = &assert_cast<const ColumnNullable &>(*source);
        data = &nullable->getNestedColumn();
    }
    const auto & strings = assert_cast<const ColumnString &>(*data);

    for (size_t i = 0; i < strings.size(); ++i)
    {
        if (nullable && nullable->isNullAt(i))
        {
            column->insertDefault();
            continue;
        }
        const std::string_view ref = strings.getDataAt(i);
        ReadBuffer in_buffer(const_cast<char *>(ref.data()), ref.size(), 0);
        GeometricObject object = geo_metadata.encoding == GeoEncoding::WKB
            ? parseWKBFormat(in_buffer) : parseWKTFormat(in_buffer);
        appendObjectToGeoColumn(object, geo_metadata.type, *column);
    }
    return column;
}

Chunk ArrowIPCBlockInputFormat::buildChunk(std::vector<ArrowIPC::RecordBatchDecoder::DecodedColumn> & decoded, size_t num_rows)
{
    const Block & header = getPort().getHeader();

    /// Map decoded column name -> index (lower-cased when case-insensitive matching is on).
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    BlockMissingValues * block_missing_values_ptr
        = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    std::unordered_map<String, size_t> name_to_index;
    name_to_index.reserve(decoded.size());
    for (size_t i = 0; i < decoded.size(); ++i)
    {
        String key = decoded[i].name;
        if (case_insensitive)
            boost::to_lower(key);
        name_to_index.emplace(std::move(key), i);
    }

    /// Cache of `Nested`/struct table extractors keyed by the (possibly lower-cased) nested table name.
    /// The backing block must outlive the extractor (it holds a reference into it), so keep both.
    std::unordered_map<String, std::pair<std::shared_ptr<Block>, std::shared_ptr<NestedColumnExtractHelper>>> nested_extractors;

    Columns columns;
    columns.reserve(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(i);
        String search_name = header_column.name;
        if (case_insensitive)
            boost::to_lower(search_name);

        ColumnWithTypeAndName column;
        auto it = name_to_index.find(search_name);
        if (it == name_to_index.end())
        {
            bool read_from_nested = false;

            /// A subcolumn of a `Nested`/struct field: `table.a` is extracted from the Arrow field `table`.
            const String nested_table_name = Nested::extractTableName(header_column.name);
            String search_nested = nested_table_name;
            if (case_insensitive)
                boost::to_lower(search_nested);

            auto nested_it = name_to_index.find(search_nested);
            if (nested_it != name_to_index.end())
            {
                auto extractor_it = nested_extractors.find(search_nested);
                if (extractor_it == nested_extractors.end())
                {
                    /// Collect the requested subcolumns into a single `Nested` type and reshape the decoded
                    /// column to it, so `Nested::flatten` (used by the extractor) recognises and splits it
                    /// — mirroring how the library reader reads the field with this type hint.
                    NamesAndTypesList nested_columns;
                    for (const auto & name_and_type : header.getNamesAndTypesList())
                        if (name_and_type.name.starts_with(nested_table_name + "."))
                            nested_columns.push_back(name_and_type);

                    auto & src = decoded[nested_it->second];
                    ColumnWithTypeAndName nested_column(src.column, src.type, nested_table_name);
                    const auto collected = Nested::collect(nested_columns);
                    if (!collected.empty())
                    {
                        const DataTypePtr & nested_table_type = collected.front().type;
                        nested_column.column = castColumn(nested_column, nested_table_type);
                        nested_column.type = nested_table_type;
                    }
                    auto block = std::make_shared<Block>(Block({std::move(nested_column)}));
                    auto helper = std::make_shared<NestedColumnExtractHelper>(*block, case_insensitive);
                    extractor_it = nested_extractors.emplace(search_nested, std::make_pair(block, helper)).first;
                }
                if (auto nested_column = extractor_it->second.second->extractColumn(search_name))
                {
                    column = *nested_column;
                    if (case_insensitive)
                        column.name = header_column.name;
                    read_from_nested = true;
                }
            }

            if (!read_from_nested)
            {
                if (!format_settings.arrow.allow_missing_columns)
                    throw Exception(
                        ErrorCodes::THERE_IS_NO_COLUMN,
                        "Column '{}' is not present in the Arrow IPC data", header_column.name);

                auto missing = header_column.type->createColumn();
                missing->insertManyDefaults(num_rows);
                if (block_missing_values_ptr)
                    block_missing_values_ptr->setBits(i, num_rows);
                columns.push_back(std::move(missing));
                continue;
            }
        }
        else
        {
            auto & src = decoded[it->second];
            column = ColumnWithTypeAndName(src.column, src.type, src.name);
        }

        /// GeoParquet: parse the WKB/WKT binary column into the geo type (declared in the schema-level
        /// "geo" metadata, or implied by a `Geometry` header type hint).
        auto geo_it = geo_columns.find(header_column.name);
        if (geo_it != geo_columns.end())
        {
            column.column = decodeGeoColumn(column.column, geo_it->second);
            column.type = getGeoDataType(geo_it->second.type);
        }
        else if (format_settings.parquet.allow_geoparquet_parser && header_column.type->getName() == "Geometry")
        {
            const GeoColumnMetadata mixed{GeoEncoding::WKB, GeoType::Mixed};
            column.column = decodeGeoColumn(column.column, mixed);
            column.type = getGeoDataType(GeoType::Mixed);
        }
        else
        {
            reinterpretFixedSizeBinary(column, header_column.type);

            /// Replace nulls coming from a nullable Arrow column with the type default and mark the missing
            /// positions, so the engine later applies the column DEFAULT expression. Handles nested cases too.
            if (format_settings.null_as_default)
                insertNullAsDefaultIfNeeded(column, header_column, i, block_missing_values_ptr);
        }

        try
        {
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while converting column {} from type {} to type {}",
                backQuote(header_column.name), column.type->getName(), header_column.type->getName()));
            throw;
        }
        columns.push_back(std::move(column.column));
    }

    return Chunk(std::move(columns), num_rows);
}

Chunk ArrowIPCBlockInputFormat::readStream()
{
    const size_t batch_start = in->count();

    while (true)
    {
        ArrowIPC::MessageReader::Message msg;
        if (!message_reader->readNextMessage(msg))
        {
            /// Drain the buffer so the underlying stream is fully consumed (HTTP keepalive, etc.).
            in->eof();
            return {};
        }

        switch (msg.header->header_type())
        {
            case ArrowIPC::flatbuf::MessageHeader_RecordBatch:
            {
                const auto * batch = msg.header->header_as_RecordBatch();
                const size_t num_rows = static_cast<size_t>(batch->length());

                if (need_only_count)
                {
                    message_reader->skipBody(msg.body_length);
                    return getChunkForCount(num_rows);
                }

                message_reader->readBody(msg.body_length, body_buffer);
                auto decoded = decoder->decodeBatch(*batch, body_buffer);
                Chunk chunk = buildChunk(decoded, num_rows);

                const size_t batch_end = in->count();
                if (batch_end > batch_start)
                    approx_bytes_read_for_chunk = batch_end - batch_start;
                return chunk;
            }
            case ArrowIPC::flatbuf::MessageHeader_DictionaryBatch:
            {
                const auto * dict_batch = msg.header->header_as_DictionaryBatch();
                const int64_t id = dict_batch->id();
                auto field_it = dictionary_value_fields.find(id);
                if (field_it == dictionary_value_fields.end())
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary batch for unknown dictionary id {}", id);

                message_reader->readBody(msg.body_length, body_buffer);
                auto decoded = decoder->decodeColumns(*dict_batch->data(), body_buffer, {field_it->second});
                checkDictionaryUnique(decoded.at(0).column);
                dictionaries.set(id, decoded.at(0).column, dict_batch->isDelta());
                continue;
            }
            case ArrowIPC::flatbuf::MessageHeader_Schema:
                /// A redundant schema message; it carries no body. Ignore and continue.
                message_reader->skipBody(msg.body_length);
                continue;
            default:
                message_reader->skipBody(msg.body_length);
                continue;
        }
    }
}

Chunk ArrowIPCBlockInputFormat::readFile()
{
    if (record_batch_current >= record_batch_blocks.size())
        return {};

    const BlockInfo & block = record_batch_blocks[record_batch_current++];
    seekable->seek(block.offset, SEEK_SET);

    ArrowIPC::MessageReader::Message msg;
    if (!message_reader->readNextMessage(msg) || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_RecordBatch)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a record batch in the Arrow file");

    const auto * batch = msg.header->header_as_RecordBatch();
    const size_t num_rows = static_cast<size_t>(batch->length());

    if (need_only_count)
        return getChunkForCount(num_rows);

    message_reader->readBody(msg.body_length, body_buffer);
    auto decoded = decoder->decodeBatch(*batch, body_buffer);
    return buildChunk(decoded, num_rows);
}

Chunk ArrowIPCBlockInputFormat::read()
{
    block_missing_values.clear();

    if (!prepared)
        prepareReader();

    if (is_stopped)
        return {};

    return stream ? readStream() : readFile();
}

const BlockMissingValues * ArrowIPCBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

void ArrowIPCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    prepared = false;
    decoder.reset();
    arrow_schema.reset();
    geo_columns.clear();
    message_reader.reset();
    dictionary_value_fields.clear();
    record_batch_blocks.clear();
    record_batch_current = 0;
    seekable = nullptr;
    memory_buffer.reset();
    file_data.clear();
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
}

}

#endif
