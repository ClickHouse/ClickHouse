#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>

#if USE_ARROW

#include <Processors/Port.h>
#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/PeekableReadBuffer.h>
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
#include <Core/UUID.h>
#include <Interpreters/castColumn.h>
#include <algorithm>
#include <Common/assert_cast.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
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
bool hasUnionField(const std::vector<ArrowIPC::ArrowField> & fields)
{
    for (const auto & field : fields)
    {
        if (field.type.kind == ArrowIPC::TypeKind::Union || hasUnionField(field.type.children))
            return true;
    }
    return false;
}

bool elementNarrowsNullability(const DataTypePtr & from, const DataTypePtr & to);

/// True if anywhere strictly inside a container `from` has a Nullable element where `to` does not.
/// (Top-level nullable->non-nullable is handled natively by convertToHeaderType, so it is ignored here.)
bool hasNestedNullabilityNarrowing(const DataTypePtr & from, const DataTypePtr & to)
{
    const DataTypePtr f = removeNullable(removeLowCardinality(from));
    const DataTypePtr t = removeNullable(removeLowCardinality(to));
    const WhichDataType wf(f);
    const WhichDataType wt(t);
    if (wf.isArray() && wt.isArray())
        return elementNarrowsNullability(
            assert_cast<const DataTypeArray &>(*f).getNestedType(), assert_cast<const DataTypeArray &>(*t).getNestedType());
    if (wf.isMap() && wt.isMap())
    {
        const auto & fm = assert_cast<const DataTypeMap &>(*f);
        const auto & tm = assert_cast<const DataTypeMap &>(*t);
        return elementNarrowsNullability(fm.getKeyType(), tm.getKeyType())
            || elementNarrowsNullability(fm.getValueType(), tm.getValueType());
    }
    if (wf.isTuple() && wt.isTuple())
    {
        const auto & fe = assert_cast<const DataTypeTuple &>(*f).getElements();
        const auto & te = assert_cast<const DataTypeTuple &>(*t).getElements();
        if (fe.size() == te.size())
            for (size_t i = 0; i < fe.size(); ++i)
                if (elementNarrowsNullability(fe[i], te[i]))
                    return true;
    }
    return false;
}

bool elementNarrowsNullability(const DataTypePtr & from, const DataTypePtr & to)
{
    const DataTypePtr f = removeLowCardinality(from);
    const DataTypePtr t = removeLowCardinality(to);
    if (f->isNullable() && !t->isNullable())
        return true;
    return hasNestedNullabilityNarrowing(from, to);
}

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

bool ArrowIPCBlockInputFormat::needsLibraryFallback() const
{
    /// GeoParquet geometry columns (schema-level "geo" metadata) need WKB/WKT parsing into geo types.
    if (arrow_schema->custom_metadata.contains("geo"))
        return true;

    /// Unions map to Variant, which the native reader does not decode yet.
    if (hasUnionField(arrow_schema->fields))
        return true;

    /// `import_nested`: a Nested column (dotted name) backed by a List/Struct/Map Arrow field is flattened
    /// by the library reader; detect that and let it handle the flattening (and case-insensitive matching).
    const Block & header = getPort().getHeader();
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    std::unordered_set<String> nested_field_names;
    std::unordered_map<String, const ArrowIPC::ArrowField *> field_by_name;
    for (const auto & field : arrow_schema->fields)
    {
        const auto kind = field.type.kind;
        const bool is_nested = kind == ArrowIPC::TypeKind::List || kind == ArrowIPC::TypeKind::LargeList
            || kind == ArrowIPC::TypeKind::FixedSizeList || kind == ArrowIPC::TypeKind::Struct
            || kind == ArrowIPC::TypeKind::Map;
        String key = field.name;
        if (case_insensitive)
            boost::to_lower(key);
        if (is_nested)
            nested_field_names.insert(key);
        field_by_name.emplace(key, &field);
    }
    for (const auto & column : header)
    {
        const auto dot = column.name.find('.');
        if (dot != String::npos)
        {
            String prefix = column.name.substr(0, dot);
            if (case_insensitive)
                boost::to_lower(prefix);
            if (nested_field_names.contains(prefix))
                return true;
        }

        /// null_as_default reading a nullable Arrow column into a non-nullable ClickHouse column: the
        /// library replaces nulls with the column's DEFAULT expression (not just the type default), so
        /// delegate the whole column to it. Covers both the top-level and nested cases.
        if (format_settings.null_as_default)
        {
            String key = column.name;
            if (case_insensitive)
                boost::to_lower(key);
            auto it = field_by_name.find(key);
            if (it != field_by_name.end())
            {
                const DataTypePtr natural = ArrowIPC::fieldToCHType(*it->second, format_settings, it->second->nullable);
                const bool top_level_narrows = natural->isNullable() && !removeLowCardinality(column.type)->isNullable();
                if (top_level_narrows || hasNestedNullabilityNarrowing(natural, column.type))
                    return true;
            }
        }
    }
    return false;
}

void ArrowIPCBlockInputFormat::prepareReader()
{
    if (stream)
        prepareStreamReader();
    else
        prepareFileReader();

    if (needsLibraryFallback())
    {
        auto header = std::make_shared<const Block>(getPort().getHeader());
        if (stream)
        {
            peekable->rollbackToCheckpoint(/*drop=*/true);
            fallback = std::make_unique<ArrowBlockInputFormat>(*peekable, header, /*stream_=*/true, format_settings);
        }
        else
        {
            seekable->seek(0, SEEK_SET);
            fallback = std::make_unique<ArrowBlockInputFormat>(*seekable, header, /*stream_=*/false, format_settings);
        }
        message_reader.reset();
        prepared = true;
        return;
    }

    if (stream)
        peekable->dropCheckpoint();

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
    /// Read the schema behind a checkpoint so the stream can be rewound for the library fallback.
    peekable = std::make_unique<PeekableReadBuffer>(*in);
    peekable->setCheckpoint();
    message_reader.emplace(*peekable);

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

ColumnPtr ArrowIPCBlockInputFormat::convertToHeaderType(
    const ColumnPtr & column, const DataTypePtr & from_type, const DataTypePtr & to_type, const String & name)
{
    const DataTypePtr target_no_null = removeNullable(to_type);
    const WhichDataType target(target_no_null);

    /// A fixed_size_binary(16) read into a UUID column (e.g. an external file without the arrow.uuid
    /// extension): reinterpret the 16 bytes with the same half-reversal the library import uses.
    if (target.isUUID())
    {
        const ColumnPtr nested = column->isNullable()
            ? assert_cast<const ColumnNullable &>(*column).getNestedColumnPtr() : column;
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
            if (column->isNullable())
            {
                result = ColumnNullable::create(result, assert_cast<const ColumnNullable &>(*column).getNullMapColumnPtr());
                result_type = std::make_shared<DataTypeNullable>(target_no_null);
            }
            return castColumn({result, result_type, name}, to_type);
        }
    }

    /// Big integers are decoded as FixedString (their Arrow fixed_size_binary representation); castColumn
    /// would try to parse them as text, so reinterpret the raw little-endian bytes instead.
    const bool target_is_big_int = target.isInt128() || target.isUInt128() || target.isInt256() || target.isUInt256();
    if (target_is_big_int)
    {
        ColumnPtr null_map;
        const ColumnPtr nested = column->isNullable()
            ? assert_cast<const ColumnNullable &>(*column).getNestedColumnPtr()
            : column;
        if (column->isNullable())
            null_map = assert_cast<const ColumnNullable &>(*column).getNullMapColumnPtr();

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
                /// Reconcile nullability with the requested type (cheap when already equal).
                return castColumn({result, result_type, name}, to_type);
            }
        }
    }

    /// Nullable->non-nullable narrowing under input_format_null_as_default is handled by delegating the
    /// whole read to the library reader (see needsLibraryFallback), which applies column DEFAULT
    /// expressions; so here a plain cast is sufficient.
    return castColumn({column, from_type, name}, to_type);
}

Chunk ArrowIPCBlockInputFormat::buildChunk(std::vector<ArrowIPC::RecordBatchDecoder::DecodedColumn> & decoded, size_t num_rows)
{
    const Block & header = getPort().getHeader();

    /// Map decoded column name -> index (lower-cased when case-insensitive matching is on).
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    std::unordered_map<String, size_t> name_to_index;
    name_to_index.reserve(decoded.size());
    for (size_t i = 0; i < decoded.size(); ++i)
    {
        String key = decoded[i].name;
        if (case_insensitive)
            boost::to_lower(key);
        name_to_index.emplace(std::move(key), i);
    }

    Columns columns;
    columns.reserve(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(i);
        String key = header_column.name;
        if (case_insensitive)
            boost::to_lower(key);

        auto it = name_to_index.find(key);
        if (it != name_to_index.end())
        {
            auto & src = decoded[it->second];
            columns.push_back(convertToHeaderType(src.column, src.type, header_column.type, src.name));
        }
        else if (format_settings.arrow.allow_missing_columns)
        {
            auto column = header_column.type->createColumn();
            column->insertManyDefaults(num_rows);
            if (format_settings.defaults_for_omitted_fields)
                block_missing_values.setBits(i, num_rows);
            columns.push_back(std::move(column));
        }
        else
        {
            throw Exception(
                ErrorCodes::THERE_IS_NO_COLUMN,
                "Column '{}' is not present in the Arrow IPC data", header_column.name);
        }
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

    if (fallback)
        return fallback->read();

    return stream ? readStream() : readFile();
}

const BlockMissingValues * ArrowIPCBlockInputFormat::getMissingValues() const
{
    return fallback ? fallback->getMissingValues() : &block_missing_values;
}

void ArrowIPCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    prepared = false;
    fallback.reset();
    peekable.reset();
    decoder.reset();
    arrow_schema.reset();
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
