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
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
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

namespace
{
/// Structural equality of two parsed Arrow types (kind plus every type attribute, recursing into
/// children). Used to verify that a reused Arrow dictionary id describes the same value type.
bool arrowTypesEqual(const ArrowIPC::ArrowType & a, const ArrowIPC::ArrowType & b);

/// Equality of two Arrow fields. `compare_name` is false at the top level (a dictionary's value field is
/// named after its column, and two columns may share a dictionary id) but true for nested children, whose
/// names are part of the value type (e.g. Tuple element names).
bool arrowFieldsEqual(const ArrowIPC::ArrowField & a, const ArrowIPC::ArrowField & b, bool compare_name)
{
    if (compare_name && a.name != b.name)
        return false;
    if (a.nullable != b.nullable)
        return false;
    if (a.dictionary != b.dictionary)
        return false;
    /// Field metadata changes the mapped ClickHouse type for the same physical Arrow type (e.g. the Arrow
    /// extension name maps `fixed_size_binary(16)` to `UUID` instead of `FixedString(16)`), so two fields
    /// reusing a dictionary id must carry the same metadata, not only the same physical type.
    if (a.custom_metadata != b.custom_metadata)
        return false;
    return arrowTypesEqual(a.type, b.type);
}

bool arrowTypesEqual(const ArrowIPC::ArrowType & a, const ArrowIPC::ArrowType & b)
{
    if (a.kind != b.kind || a.bit_width != b.bit_width || a.is_signed != b.is_signed
        || a.float_precision != b.float_precision || a.precision != b.precision || a.scale != b.scale
        || a.decimal_bit_width != b.decimal_bit_width || a.unit != b.unit || a.time_bit_width != b.time_bit_width
        || a.timezone != b.timezone || a.byte_width != b.byte_width || a.list_size != b.list_size
        || a.union_mode != b.union_mode || a.union_type_ids != b.union_type_ids
        || a.unsupported_type_name != b.unsupported_type_name || a.skip_layout != b.skip_layout
        || a.children.size() != b.children.size())
        return false;
    for (size_t i = 0; i < a.children.size(); ++i)
    {
        if (!arrowFieldsEqual(a.children[i], b.children[i], /*compare_name=*/true))
            return false;
    }
    return true;
}
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

void ArrowIPCBlockInputFormat::collectDictionaryFields(const ArrowIPC::ArrowFields & fields)
{
    for (const auto & field : fields)
    {
        if (field.dictionary)
        {
            /// The dictionary batch carries the plain value column: same type, but not dictionary-encoded.
            ArrowIPC::ArrowField value_field = field;
            value_field.dictionary.reset();
            const Int64 id = field.dictionary->id;
            auto it = dictionary_value_fields.find(id);
            if (it == dictionary_value_fields.end())
            {
                dictionary_value_fields.emplace(id, std::move(value_field));
            }
            /// Arrow dictionary ids are global within the file/stream: reusing one is valid only when every
            /// field describes the same dictionary value type. Reject a mismatch instead of silently
            /// overwriting the value schema — otherwise the shared `DictionaryBatch` would be decoded with
            /// one field's value type and materialized under another's (e.g. `Int32` values surfaced as a
            /// `LowCardinality(Date32)` column), bypassing validation or tripping type assertions.
            else if (!arrowFieldsEqual(it->second, value_field, /*compare_name=*/false))
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC dictionary id {} is used by fields with different value types", id);
            }
        }
        collectDictionaryFields(field.type.children);
    }
}

namespace
{
/// When case-insensitive column matching is enabled, the named-tuple CAST that maps a decoded Arrow
/// struct onto the requested type matches element names case-sensitively, so differently-cased struct
/// fields would silently become default values (the library reader instead matches them). Rebuild the
/// decoded type so that every Tuple element name matching a requested element name case-insensitively is
/// renamed to the requested (exact) name; the subsequent CAST then matches. Only the type is rebuilt —
/// Tuple element names live in the type, not the column, so the column data is left untouched. Recurses
/// through Array / Map / Nullable / LowCardinality wrappers.
DataTypePtr alignStructFieldNamesCaseInsensitive(const DataTypePtr & from, const DataTypePtr & to)
{
    if (const auto * to_low = typeid_cast<const DataTypeLowCardinality *>(to.get()))
    {
        if (const auto * from_low = typeid_cast<const DataTypeLowCardinality *>(from.get()))
            return std::make_shared<DataTypeLowCardinality>(
                alignStructFieldNamesCaseInsensitive(from_low->getDictionaryType(), to_low->getDictionaryType()));
        return from;
    }
    if (const auto * to_null = typeid_cast<const DataTypeNullable *>(to.get()))
    {
        if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.get()))
            return std::make_shared<DataTypeNullable>(
                alignStructFieldNamesCaseInsensitive(from_null->getNestedType(), to_null->getNestedType()));
        return alignStructFieldNamesCaseInsensitive(from, to_null->getNestedType());
    }
    if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.get()))
        return std::make_shared<DataTypeNullable>(alignStructFieldNamesCaseInsensitive(from_null->getNestedType(), to));

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to.get()))
    {
        if (const auto * from_arr = typeid_cast<const DataTypeArray *>(from.get()))
            return std::make_shared<DataTypeArray>(
                alignStructFieldNamesCaseInsensitive(from_arr->getNestedType(), to_arr->getNestedType()));
        return from;
    }
    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        if (const auto * from_map = typeid_cast<const DataTypeMap *>(from.get()))
            return std::make_shared<DataTypeMap>(
                alignStructFieldNamesCaseInsensitive(from_map->getKeyType(), to_map->getKeyType()),
                alignStructFieldNamesCaseInsensitive(from_map->getValueType(), to_map->getValueType()));
        return from;
    }
    if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get()))
    {
        const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from.get());
        if (!from_tuple || !from_tuple->hasExplicitNames() || !to_tuple->hasExplicitNames())
            return from;

        const auto & from_elems = from_tuple->getElements();
        const auto & from_names = from_tuple->getElementNames();
        const auto & to_elems = to_tuple->getElements();
        const auto & to_names = to_tuple->getElementNames();

        UnorderedMapWithMemoryTracking<String, size_t> to_by_lower;
        to_by_lower.reserve(to_names.size());
        for (size_t i = 0; i < to_names.size(); ++i)
            to_by_lower.emplace(boost::to_lower_copy(to_names[i]), i); /// first occurrence wins on duplicates

        DataTypes new_elems;
        Strings new_names;
        new_elems.reserve(from_elems.size());
        new_names.reserve(from_names.size());
        for (size_t i = 0; i < from_elems.size(); ++i)
        {
            auto it = to_by_lower.find(boost::to_lower_copy(from_names[i]));
            if (it != to_by_lower.end())
            {
                new_names.push_back(to_names[it->second]);
                new_elems.push_back(alignStructFieldNamesCaseInsensitive(from_elems[i], to_elems[it->second]));
            }
            else
            {
                new_names.push_back(from_names[i]);
                new_elems.push_back(from_elems[i]);
            }
        }
        return std::make_shared<DataTypeTuple>(new_elems, new_names);
    }
    return from;
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

    UnorderedSetWithMemoryTracking<std::string_view> seen;
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

/// Collects every Arrow dictionary id used anywhere in `field`'s type subtree (the field itself or a
/// dictionary nested inside its Array/Map/Tuple/Union children). Used to decide which DictionaryBatch
/// bodies a subset read actually needs.
void collectDictionaryIdsInSubtree(const ArrowIPC::ArrowField & field, UnorderedSetWithMemoryTracking<Int64> & out)
{
    if (field.dictionary)
        out.insert(field.dictionary->id);
    for (const auto & child : field.type.children)
        collectDictionaryIdsInSubtree(child, out);
}
}

void ArrowIPCBlockInputFormat::prepareReader()
{
    /// Determine which top-level Arrow fields the requested header needs, so the reader can skip the rest
    /// (subset-of-columns support): a field is needed if a header column matches it by name, or is a
    /// `Nested`/struct subcolumn of it (a header column `name.sub` keeps the field `name`) — mirroring how
    /// `buildChunk` maps columns. This depends only on the header, so compute it before reading the file:
    /// the file reader uses it below to skip the dictionaries of unrequested columns. Names are lower-cased
    /// when case-insensitive matching is on, matching the lookup in `decodeColumns`.
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    auto normalize = [&](String s) -> String { if (case_insensitive) boost::to_lower(s); return s; };
    requested_top_level_fields.clear();
    requested_field_target_types.clear();
    for (const auto & header_column : getPort().getHeader())
    {
        requested_top_level_fields.insert(normalize(header_column.name));
        requested_top_level_fields.insert(normalize(Nested::extractTableName(header_column.name)));
        /// Record each requested column's target type by (normalized) name so the decoder can read a
        /// `date32` mapped to a numeric target as the raw day number — recursively for a `date32` nested in
        /// an Array/Tuple/Map, and for one addressed as a subcolumn (e.g. `t.d`). Mirrors the Apache Arrow
        /// library reader's recursive numeric type-hint handling.
        requested_field_target_types[normalize(header_column.name)] = header_column.type;
    }

    if (stream)
        prepareStreamReader();
    else
        prepareFileReader();

    /// The read was cancelled while buffering a non-seekable input (`prepareFileReader` bailed out before
    /// parsing): leave `arrow_schema` unset and return without dereferencing it. `read` then sees
    /// `is_stopped` and returns an empty chunk — a clean cancellation rather than a truncated-file error.
    if (is_stopped)
        return;

    /// GeoParquet geometry columns (schema-level "geo" metadata) are decoded from WKB/WKT into geo types.
    if (format_settings.parquet.allow_geoparquet_parser)
    {
        auto geo_it = arrow_schema->custom_metadata.find("geo");
        geo_columns = parseGeoMetadataEncoding(
            geo_it != arrow_schema->custom_metadata.end() ? &geo_it->second : nullptr);
    }

    /// Reject duplicate column names, matching the Apache Arrow library based reader.
    UnorderedSetWithMemoryTracking<String> seen_names;
    for (const auto & field : arrow_schema->fields)
    {
        if (!seen_names.insert(field.name).second)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Duplicate column '{}' in the Arrow schema", field.name);
    }

    collectDictionaryFields(arrow_schema->fields);
    decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings, dictionaries);

    /// Collect the dictionary ids the requested columns actually reference, so the stream reader can skip
    /// the DictionaryBatch bodies of dictionaries used only by unrequested columns. (The file reader has
    /// already computed and used this set above, before decoding its dictionaries up front; recomputing it
    /// here is cheap and keeps the stream path — whose dictionaries are decoded lazily while reading —
    /// correct.)
    computeReachableDictionaryIds();

    prepared = true;
}

void ArrowIPCBlockInputFormat::computeReachableDictionaryIds()
{
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    reachable_dictionary_ids.clear();
    for (const auto & field : arrow_schema->fields)
    {
        String name = field.name;
        if (case_insensitive)
            boost::to_lower(name);
        /// A top-level field is kept iff its normalized name is requested — the same condition
        /// `decodeColumns` uses to decide whether to decode or skip it. Every dictionary in a kept field's
        /// subtree is reachable; dictionaries referenced only by skipped fields are not.
        if (requested_top_level_fields.contains(name))
            collectDictionaryIdsInSubtree(field, reachable_dictionary_ids);
    }
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
    chassert(in); /// the input buffer is set in the constructor and is never null
    size_t file_size = 0;
    /// Use random access only when the input is genuinely seekable, its size is known, and
    /// `input_format_allow_seeks` is not disabled — mirroring `asArrowFile` in the library reader.
    seekable = format_settings.seekable_read ? dynamic_cast<SeekableReadBuffer *>(in) : nullptr;
    std::optional<size_t> known_size;
    if (seekable)
        known_size = tryGetFileSizeFromReadBuffer(*in);
    if (seekable && known_size && *known_size > 0 && seekable->checkIfActuallySeekable())
    {
        file_size = *known_size;
    }
    else
    {
        /// Non-seekable input: the file format needs the whole file in memory (its footer is at the end).
        /// Validate the leading "ARROW1" magic before buffering the (possibly huge, possibly non-Arrow)
        /// remainder, and copy it with a cancellation-aware loop — matching the Apache Arrow library
        /// reader's fail-fast and cancellation behavior instead of slurping the entire stream first.
        static constexpr std::string_view ARROW_FILE_MAGIC = "ARROW1";
        char magic[ARROW_FILE_MAGIC.size()] = {};
        in->readStrict(magic, sizeof(magic));
        if (std::string_view(magic, sizeof(magic)) != ARROW_FILE_MAGIC)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: the leading {} magic is missing", ARROW_FILE_MAGIC);
        file_data.assign(magic, sizeof(magic));
        WriteBufferFromString out(file_data, AppendModeTag{});
        copyData(*in, out, is_stopped);
        out.finalize();
        /// `copyData` returns early when the read was cancelled, leaving `file_data` partial. Do not parse
        /// that truncated buffer as an Arrow file — bail out and let `prepareReader`/`read` finish cleanly
        /// (matching the library path, which returned without parsing after a cancelled `asArrowFile`).
        if (is_stopped)
            return;
        file_size = file_data.size();
        auto in_memory = std::make_unique<ReadBufferFromMemory>(file_data.data(), file_data.size());
        seekable = in_memory.get(); /// `ReadBufferFromMemory` is a `SeekableReadBuffer` (implicit upcast).
        memory_buffer = std::move(in_memory);
    }
    message_reader.emplace(*seekable);

    ArrowIPC::ArrowFileFooter footer = ArrowIPC::readArrowFileFooter(*seekable, file_size);
    arrow_schema = std::move(footer.schema);
    for (const auto & block : footer.record_batch_blocks)
        record_batch_blocks.push_back({block.offset, block.metadata_length, block.body_length});

    /// For count-only reads, the row count comes from the record-batch metadata (`batch->length()`), so
    /// no record batch is ever decoded and the dictionaries are never needed. Skip decoding the
    /// dictionary batches up front: it is wasted work, materializes potentially large dictionaries, and
    /// would needlessly fail on a corrupt dictionary body when the counts are already in the footer.
    if (!need_only_count)
    {
        /// Decode all dictionary batches up front (the registry must be populated before any record batch).
        collectDictionaryFields(arrow_schema->fields);
        /// Subset reads: only the dictionaries the requested columns reference are decoded; the bodies of
        /// dictionaries used solely by unrequested top-level fields are skipped (see the loop below).
        computeReachableDictionaryIds();
        auto temp_decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings, dictionaries);
        for (const auto & block : footer.dictionary_blocks)
        {
            seekable->seek(block.offset, SEEK_SET);
            ArrowIPC::MessageReader::Message msg;
            /// Bound the metadata read by the footer block's declared metadata size, so a malformed footer
            /// cannot make the reader allocate a huge metadata buffer from the length prefix at `block.offset`.
            if (!message_reader->readNextMessage(msg, block.metadata_length)
                || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_DictionaryBatch)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a dictionary batch in the Arrow file");

            const auto * dict_batch = msg.header->header_as_DictionaryBatch();
            const Int64 id = dict_batch->id();
            /// A dictionary referenced only by unrequested (skipped) top-level fields is never used by the
            /// chunk this query builds. Do not decode its body: that would be wasted work and could fail or
            /// allocate a large/corrupt dictionary for a column the query did not ask for. The next loop
            /// iteration seeks to the following block, so the unread body is harmless. Check this before any
            /// body-length/data validation so a missing/corrupt unrequested dictionary cannot fail a
            /// projected read.
            if (!reachable_dictionary_ids.contains(id))
                continue;
            /// The footer block fixes this message's body size; a message claiming a different (e.g. huge)
            /// body length is corrupt and would otherwise force reading past the footer-declared boundary.
            if (msg.body_length != block.body_length)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC dictionary batch body length {} does not match the footer block length {}",
                    msg.body_length, block.body_length);
            /// `DictionaryBatch.data` is optional in the FlatBuffer schema; reject a missing one rather
            /// than dereferencing null below.
            if (!dict_batch->data())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary batch has no data");
            auto field_it = dictionary_value_fields.find(id);
            if (field_it == dictionary_value_fields.end())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow file dictionary batch for unknown id {}", id);

            /// Reject a batch declaring buffers/nodes beyond its single value field before materializing the
            /// body, so surplus buffers are never read or decompressed only to be ignored.
            const ArrowIPC::ArrowFields value_fields{field_it->second};
            temp_decoder->validateBatchLayout(*dict_batch->data(), value_fields);
            message_reader->readBody(*dict_batch->data(), msg.body_length, body_buffer);
            auto decoded = temp_decoder->decodeColumns(*dict_batch->data(), body_buffer, value_fields);
            checkDictionaryUnique(decoded.at(0).column);
            dictionaries.set(id, decoded.at(0).column, dict_batch->isDelta());
            /// A delta batch merges into the existing dictionary; re-validate the merged values. The
            /// per-batch check above only proves the delta is internally unique, but a unique delta can
            /// still repeat a value already present in the base dictionary, which would violate the
            /// LowCardinality dictionary uniqueness invariant the non-delta path enforces.
            if (dict_batch->isDelta())
                checkDictionaryUnique(dictionaries.get(id));
        }
    }
}

namespace
{

/// Reinterpret the raw bytes of a fixed_size_binary column (`ColumnFixedString`) as a UUID, IPv6, or big
/// integer, matching the ClickHouse Arrow writer (which stores those types as fixed_size_binary) and the
/// library reader. Returns nullptr when `to_no_null` is not one of those types. Throws on a width mismatch:
/// a wrong-width value is corrupt for these fixed-width targets (the library reader rejects it too), and
/// letting it fall through to `castColumn` would text-parse the bytes.
MutableColumnPtr reinterpretFixedStringLeaf(const ColumnFixedString & fixed, const DataTypePtr & to_no_null)
{
    const WhichDataType which(to_no_null);
    const size_t n = fixed.getN();
    const size_t rows = fixed.size();

    auto require = [&](size_t width)
    {
        if (n != width)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow fixed_size_binary of width {} cannot be read as {} (expected {})",
                n, to_no_null->getName(), width);
    };

    if (which.isUUID())
    {
        require(16);
        auto out = ColumnVector<UUID>::create(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            auto * dst = reinterpret_cast<uint8_t *>(&out->getData()[i]);
            memcpy(dst, &fixed.getChars()[i * 16], 16);
            std::reverse(dst, dst + 8);
            std::reverse(dst + 8, dst + 16);
        }
        return out;
    }

    size_t width = 0;
    if (which.isIPv6() || which.isInt128() || which.isUInt128())
        width = 16;
    else if (which.isInt256() || which.isUInt256())
        width = 32;
    else
        return nullptr;

    require(width);
    auto out = to_no_null->createColumn();
    auto copy = [&](auto & data) { data.resize(rows); if (rows) memcpy(data.data(), fixed.getChars().data(), rows * width); };
    switch (which.idx)
    {
        case TypeIndex::IPv6: copy(assert_cast<ColumnVector<IPv6> &>(*out).getData()); break;
        case TypeIndex::Int128: copy(assert_cast<ColumnVector<Int128> &>(*out).getData()); break;
        case TypeIndex::UInt128: copy(assert_cast<ColumnVector<UInt128> &>(*out).getData()); break;
        case TypeIndex::Int256: copy(assert_cast<ColumnVector<Int256> &>(*out).getData()); break;
        default: copy(assert_cast<ColumnVector<UInt256> &>(*out).getData()); break;
    }
    return out;
}

/// Reinterpret the raw bytes of a variable-width binary column (`ColumnString`) as an IPv6 or big integer,
/// matching the library reader's `readIPv6ColumnFromBinaryData` / `readColumnWithBigNumberFromBinaryData`.
/// `null_map` (may be null) marks rows skipped in the width check and defaulted in the output. Returns
/// nullptr when the target is not one of those types, or when any non-null row is not exactly the target
/// width - in that case the column is left as String for the subsequent cast (matching the library, which
/// falls back to text parsing rather than failing).
MutableColumnPtr reinterpretStringLeaf(const ColumnString & str, const NullMap * null_map, const DataTypePtr & to_no_null)
{
    const WhichDataType which(to_no_null);
    size_t width = 0;
    if (which.isIPv6() || which.isInt128() || which.isUInt128())
        width = 16;
    else if (which.isInt256() || which.isUInt256())
        width = 32;
    else
        return nullptr;

    const size_t rows = str.size();
    for (size_t i = 0; i < rows; ++i)
    {
        if (null_map && (*null_map)[i])
            continue;
        if (str.getDataAt(i).size() != width)
            return nullptr;
    }

    auto out = to_no_null->createColumn();
    out->reserve(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        if (null_map && (*null_map)[i])
        {
            out->insertDefault();
            continue;
        }
        const auto ref = str.getDataAt(i);
        out->insertData(ref.data(), ref.size());
    }
    return out;
}

/// Recursively rewrite the raw-byte leaves (fixed_size_binary / binary) of a decoded column into the
/// UUID / IPv6 / big-integer types the requested `to_type` asks for, descending through Nullable, Array,
/// Tuple and Map so nested shapes convert too. Anything not recognised is returned unchanged for the
/// subsequent `castColumn`. Returns the (possibly rewritten) column and its new type.
std::pair<ColumnPtr, DataTypePtr> reinterpretRawBytes(
    const ColumnPtr & col, const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    const DataTypePtr to_no_null = removeNullable(to_type);
    const DataTypePtr from_no_null = removeNullable(from_type);

    /// Schema inference can wrap a composite in Nullable (e.g. `Nullable(Tuple(...))`). The composite
    /// recursion below matches on the unwrapped column, so peel a Nullable column wrapper here first and
    /// restore it afterwards (dropping it if the reinterpreted composite cannot itself be Nullable).
    const bool composite_target = typeid_cast<const DataTypeArray *>(to_no_null.get())
        || typeid_cast<const DataTypeTuple *>(to_no_null.get())
        || typeid_cast<const DataTypeMap *>(to_no_null.get());
    if (composite_target)
    {
        if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(col.get()))
        {
            auto [new_nested, new_nested_type] = reinterpretRawBytes(col_nullable->getNestedColumnPtr(), from_no_null, to_no_null);
            if (new_nested.get() == col_nullable->getNestedColumnPtr().get())
                return {col, from_type};
            if (new_nested->canBeInsideNullable())
                return {ColumnNullable::create(new_nested, col_nullable->getNullMapColumnPtr()),
                        std::make_shared<DataTypeNullable>(new_nested_type)};
            return {std::move(new_nested), std::move(new_nested_type)};
        }
    }

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to_no_null.get()))
    {
        const auto * from_arr = typeid_cast<const DataTypeArray *>(from_no_null.get());
        const auto * col_arr = typeid_cast<const ColumnArray *>(col.get());
        if (!from_arr || !col_arr)
            return {col, from_type};
        auto [new_data, new_data_type] = reinterpretRawBytes(col_arr->getDataPtr(), from_arr->getNestedType(), to_arr->getNestedType());
        if (new_data.get() == col_arr->getDataPtr().get())
            return {col, from_type};
        auto new_arr = ColumnArray::create(IColumn::mutate(std::move(new_data)), IColumn::mutate(col_arr->getOffsetsPtr()));
        return {std::move(new_arr), std::make_shared<DataTypeArray>(new_data_type)};
    }

    if (const auto * to_tup = typeid_cast<const DataTypeTuple *>(to_no_null.get()))
    {
        const auto * from_tup = typeid_cast<const DataTypeTuple *>(from_no_null.get());
        const auto * col_tup = typeid_cast<const ColumnTuple *>(col.get());
        if (!from_tup || !col_tup)
            return {col, from_type};
        const auto & to_elems = to_tup->getElements();
        const auto & from_elems = from_tup->getElements();
        if (to_elems.size() != from_elems.size() || col_tup->tupleSize() != to_elems.size())
            return {col, from_type};
        Columns new_cols(to_elems.size());
        DataTypes new_types(to_elems.size());
        bool changed = false;
        for (size_t i = 0; i < to_elems.size(); ++i)
        {
            auto [c, t] = reinterpretRawBytes(col_tup->getColumnPtr(i), from_elems[i], to_elems[i]);
            if (c.get() != col_tup->getColumnPtr(i).get())
                changed = true;
            new_cols[i] = std::move(c);
            new_types[i] = std::move(t);
        }
        if (!changed)
            return {col, from_type};
        DataTypePtr new_tuple_type = from_tup->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(new_types, from_tup->getElementNames())
            : std::make_shared<DataTypeTuple>(new_types);
        return {ColumnTuple::create(new_cols), std::move(new_tuple_type)};
    }

    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to_no_null.get()))
    {
        const auto * from_map = typeid_cast<const DataTypeMap *>(from_no_null.get());
        const auto * col_map = typeid_cast<const ColumnMap *>(col.get());
        if (!from_map || !col_map)
            return {col, from_type};
        /// A Map is physically an Array(Tuple(key, value)); recurse into that shape so raw-byte keys/values
        /// convert, then rewrap.
        auto from_nested = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{from_map->getKeyType(), from_map->getValueType()}));
        auto to_nested = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{to_map->getKeyType(), to_map->getValueType()}));
        auto [new_nested, new_nested_type] = reinterpretRawBytes(col_map->getNestedColumnPtr(), from_nested, to_nested);
        if (new_nested.get() == col_map->getNestedColumnPtr().get())
            return {col, from_type};
        const auto & new_tuple = assert_cast<const DataTypeTuple &>(*assert_cast<const DataTypeArray &>(*new_nested_type).getNestedType());
        auto new_map_type = std::make_shared<DataTypeMap>(new_tuple.getElement(0), new_tuple.getElement(1));
        return {ColumnMap::create(new_nested), std::move(new_map_type)};
    }

    const WhichDataType which(to_no_null);
    const bool raw_target = which.isUUID() || which.isIPv6()
        || which.isInt128() || which.isUInt128() || which.isInt256() || which.isUInt256();
    if (!raw_target)
        return {col, from_type};

    const auto * nullable = typeid_cast<const ColumnNullable *>(col.get());
    const IColumn & nested = nullable ? nullable->getNestedColumn() : *col;
    const NullMap * null_map = nullable ? &nullable->getNullMapData() : nullptr;

    MutableColumnPtr typed;
    if (const auto * fixed = typeid_cast<const ColumnFixedString *>(&nested))
        typed = reinterpretFixedStringLeaf(*fixed, to_no_null);
    else if (const auto * str = typeid_cast<const ColumnString *>(&nested))
        typed = reinterpretStringLeaf(*str, null_map, to_no_null);
    if (!typed)
        return {col, from_type};

    ColumnPtr result = std::move(typed);
    DataTypePtr result_type = to_no_null;
    if (nullable)
    {
        result = ColumnNullable::create(result, nullable->getNullMapColumnPtr());
        result_type = std::make_shared<DataTypeNullable>(to_no_null);
    }
    return {std::move(result), std::move(result_type)};
}

}

void ArrowIPCBlockInputFormat::reinterpretRawByteColumns(ColumnWithTypeAndName & column, const DataTypePtr & to_type)
{
    auto [new_column, new_type] = reinterpretRawBytes(column.column, column.type, to_type);
    column.column = std::move(new_column);
    column.type = std::move(new_type);
}

ColumnPtr ArrowIPCBlockInputFormat::decodeGeoColumn(const ColumnPtr & source, const GeoColumnMetadata & geo_metadata, bool precise_float_parsing)
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
    /// The schema-level "geo" metadata is untrusted: a file may tag a non-binary field (e.g. an Int32 or a
    /// list) as geometry. Reject it with a clean INCORRECT_DATA instead of letting the cast below misbehave
    /// (a bad-cast exception in checked builds, undefined behavior in release). WKB/WKT geometry is always a
    /// variable-length binary column, which decodes to ColumnString.
    const auto * strings_ptr = typeid_cast<const ColumnString *>(data);
    if (!strings_ptr)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow column tagged as geometry in the \"geo\" metadata must decode to a binary/string column, but got {}",
            data->getName());
    const auto & strings = *strings_ptr;

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
            ? parseWKBFormat(in_buffer) : parseWKTFormat(in_buffer, precise_float_parsing);
        appendObjectToGeoColumn(object, geo_metadata.type, *column);
    }
    return column;
}

Chunk ArrowIPCBlockInputFormat::buildChunk(ArrowIPC::RecordBatchDecoder::DecodedColumns & decoded, size_t num_rows)
{
    const Block & header = getPort().getHeader();

    /// Map decoded column name -> index (lower-cased when case-insensitive matching is on).
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    BlockMissingValues * block_missing_values_ptr
        = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    UnorderedMapWithMemoryTracking<String, size_t> name_to_index;
    name_to_index.reserve(decoded.size());
    for (size_t i = 0; i < decoded.size(); ++i)
    {
        String key = decoded[i].name;
        if (case_insensitive)
            boost::to_lower(key);
        /// With case-insensitive matching, names like `A` and `a` fold to the same key. Keep the last
        /// occurrence (`insert_or_assign`, not `emplace`) so the native reader selects the same column the
        /// Apache Arrow library path did for the same schema and setting.
        name_to_index.insert_or_assign(std::move(key), i);
    }

    /// Cache of `Nested`/struct table extractors keyed by the (possibly lower-cased) nested table name.
    /// The backing block must outlive the extractor (it holds a reference into it), so keep both.
    UnorderedMapWithMemoryTracking<String, std::pair<std::shared_ptr<Block>, std::shared_ptr<NestedColumnExtractHelper>>> nested_extractors;

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
                    {
                        if (name_and_type.name.starts_with(nested_table_name + "."))
                            nested_columns.push_back(name_and_type);
                    }

                    auto & src = decoded[nested_it->second];
                    ColumnWithTypeAndName nested_column(src.column, src.type, nested_table_name);

                    /// Arrow's default nullable schema yields `Array(Nullable(Tuple(...)))`. `Nested::flatten`
                    /// cannot split a Nullable tuple, and casting it onto the non-nullable Nested tuple would
                    /// fail on struct-level nulls. Unwrap the nullable tuple, propagating the struct null map
                    /// down to each element (matching the library reader), so it becomes `Array(Tuple(...))`.
                    if (const auto * arr_type = typeid_cast<const DataTypeArray *>(nested_column.type.get());
                        arr_type && typeid_cast<const DataTypeTuple *>(removeNullable(arr_type->getNestedType()).get()))
                    {
                        const auto & arr_col = assert_cast<const ColumnArray &>(*nested_column.column);
                        auto unwrapped = Nested::unwrapNullableTuple(
                            {arr_col.getDataPtr(), arr_type->getNestedType(), nested_table_name});
                        nested_column.column = ColumnArray::create(unwrapped.column, arr_col.getOffsetsPtr());
                        nested_column.type = std::make_shared<DataTypeArray>(unwrapped.type);
                    }

                    const auto collected = Nested::collect(nested_columns);
                    if (!collected.empty())
                    {
                        const DataTypePtr & nested_table_type = collected.front().type;
                        if (case_insensitive)
                            nested_column.type = alignStructFieldNamesCaseInsensitive(nested_column.type, nested_table_type);
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
            column.column = decodeGeoColumn(column.column, geo_it->second, format_settings.precise_float_parsing);
            column.type = getGeoDataType(geo_it->second.type);
        }
        else if (format_settings.parquet.allow_geoparquet_parser && header_column.type->getName() == "Geometry")
        {
            const GeoColumnMetadata mixed{GeoEncoding::WKB, GeoType::Mixed};
            column.column = decodeGeoColumn(column.column, mixed, format_settings.precise_float_parsing);
            column.type = getGeoDataType(GeoType::Mixed);
        }
        else
        {
            reinterpretRawByteColumns(column, header_column.type);

            /// Replace nulls coming from a nullable Arrow column with the type default and mark the missing
            /// positions, so the engine later applies the column DEFAULT expression. Handles nested cases too.
            if (format_settings.null_as_default)
                insertNullAsDefaultIfNeeded(column, header_column, i, block_missing_values_ptr);
        }

        /// Match differently-cased struct field names against the requested type when case-insensitive
        /// column matching is enabled, so the named-tuple CAST below does not turn them into defaults.
        if (case_insensitive)
            column.type = alignStructFieldNamesCaseInsensitive(column.type, header_column.type);

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
                if (batch->length() < 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", batch->length());
                const size_t num_rows = static_cast<size_t>(batch->length());

                if (need_only_count)
                {
                    message_reader->skipBody(msg.body_length);
                    return getChunkForCount(num_rows);
                }

                /// Subset read: read, validate and decompress only the buffers the requested columns
                /// reference; unrequested columns' body ranges are skipped (see `readBody`).
                const auto reachable = decoder->reachableTopLevelBuffers(*batch, &requested_top_level_fields);
                message_reader->readBody(*batch, msg.body_length, body_buffer, &reachable);
                auto decoded = decoder->decodeBatch(*batch, body_buffer, &requested_top_level_fields, &requested_field_target_types, &reachable);
                Chunk chunk = buildChunk(decoded, num_rows);

                const size_t batch_end = in->count();
                if (batch_end > batch_start)
                    approx_bytes_read_for_chunk = batch_end - batch_start;
                return chunk;
            }
            case ArrowIPC::flatbuf::MessageHeader_DictionaryBatch:
            {
                /// Count-only never decodes a record batch, so the dictionaries are not needed; skip the
                /// body instead of decoding (and possibly failing on) it.
                if (need_only_count)
                {
                    message_reader->skipBody(msg.body_length);
                    continue;
                }
                const auto * dict_batch = msg.header->header_as_DictionaryBatch();
                const Int64 id = dict_batch->id();
                /// Subset reads: a dictionary referenced only by unrequested (skipped) top-level fields is
                /// never used, so skip its body instead of decoding (and possibly failing/allocating on) it.
                /// Check this before validating `data` so a missing/corrupt unrequested dictionary cannot fail
                /// a projected read.
                if (!reachable_dictionary_ids.contains(id))
                {
                    message_reader->skipBody(msg.body_length);
                    continue;
                }
                /// `DictionaryBatch.data` is optional in the FlatBuffer schema; reject a missing one rather
                /// than dereferencing null below.
                if (!dict_batch->data())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary batch has no data");
                auto field_it = dictionary_value_fields.find(id);
                if (field_it == dictionary_value_fields.end())
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary batch for unknown dictionary id {}", id);

                /// Reject a batch declaring buffers/nodes beyond its single value field before materializing
                /// the body, so surplus buffers are never read or decompressed only to be ignored.
                const ArrowIPC::ArrowFields value_fields{field_it->second};
                decoder->validateBatchLayout(*dict_batch->data(), value_fields);
                message_reader->readBody(*dict_batch->data(), msg.body_length, body_buffer);
                auto decoded = decoder->decodeColumns(*dict_batch->data(), body_buffer, value_fields);
                checkDictionaryUnique(decoded.at(0).column);
                dictionaries.set(id, decoded.at(0).column, dict_batch->isDelta());
                /// A delta batch merges into the existing dictionary; re-validate the merged values. The
                /// per-batch check above only proves the delta is internally unique, but a unique delta can
                /// still repeat a value already present in the base dictionary, which would violate the
                /// LowCardinality dictionary uniqueness invariant the non-delta path enforces.
                if (dict_batch->isDelta())
                    checkDictionaryUnique(dictionaries.get(id));
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
    /// Bound the metadata read by the footer block's declared metadata size (see the dictionary loop).
    if (!message_reader->readNextMessage(msg, block.metadata_length) || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_RecordBatch)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a record batch in the Arrow file");
    /// The footer block fixes this message's body size; a message claiming a different (e.g. huge) body
    /// length is corrupt and would otherwise force a large allocation reading past the block boundary.
    if (msg.body_length != block.body_length)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC record batch body length {} does not match the footer block length {}",
            msg.body_length, block.body_length);

    const auto * batch = msg.header->header_as_RecordBatch();
    if (batch->length() < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", batch->length());
    const size_t num_rows = static_cast<size_t>(batch->length());

    if (need_only_count)
        return getChunkForCount(num_rows);

    /// Subset read: read, validate and decompress only the buffers the requested columns reference;
    /// unrequested columns' body ranges are skipped (see `readBody`).
    const auto reachable = decoder->reachableTopLevelBuffers(*batch, &requested_top_level_fields);
    message_reader->readBody(*batch, msg.body_length, body_buffer, &reachable);
    auto decoded = decoder->decodeBatch(*batch, body_buffer, &requested_top_level_fields, &requested_field_target_types, &reachable);
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
    dictionaries.clear();
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
