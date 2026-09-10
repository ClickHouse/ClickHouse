#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Common/Exception.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Formats/FormatSettings.h>
#include <Formats/ParseError.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <absl/strings/str_split.h>
#include <base/EnumReflection.h>
#include <base/demangle.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/assert_cast.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <base/types.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool escape_variant_subcolumn_filenames;
    extern const MergeTreeSettingsBool share_nested_offsets;
    extern const MergeTreeSettingsMergeTreeSubstreamNamingVersion substream_naming_version;
}

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

void throwEmptySerializationState(const ISerialization * serialization)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Got empty state for {}", demangle(typeid(*serialization).name()));
}

void throwInvalidSerializationState(const ISerialization * serialization, const std::type_info & expected, const std::type_info & got)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Invalid State for {}. Expected: {}, got {}",
            demangle(typeid(*serialization).name()),
            demangle(expected.name()),
            demangle(got.name()));
}

UInt128 ISerialization::getHash() const
{
    if (!cached_hash)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash is not set for serialization {}", typeid(*this).name());
    return *cached_hash;
}

SerializationPtr ISerialization::pooled(UInt128 hash, absl::FunctionRef<ISerialization *()> creator)
{
    return SerializationObjectPool::getOrCreate(hash, [&]() -> ISerialization *
    {
        auto * obj = creator();
        obj->cached_hash = hash;
        return obj;
    });
}

ISerialization::KindStack ISerialization::getKindStack(const IColumn & column)
{
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column))
    {
        auto kind_stack = getKindStack(*column_sparse->getValuesPtr());
        kind_stack.push_back(Kind::SPARSE);
        return kind_stack;
    }

    if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(&column))
    {
        auto kind_stack = getKindStack(*column_replicated->getNestedColumn());
        kind_stack.push_back(Kind::REPLICATED);
        return kind_stack;
    }

    if (const auto * column_blob = typeid_cast<const ColumnBLOB *>(&column))
    {
        auto kind_stack = getKindStack(*column_blob->getWrappedColumn());
        kind_stack.push_back(Kind::DETACHED);
        return kind_stack;
    }

    return {Kind::DEFAULT};
}

static String kindToString(ISerialization::Kind kind)
{
    switch (kind)
    {
        case ISerialization::Kind::DEFAULT:
            return "Default";
        case ISerialization::Kind::SPARSE:
            return "Sparse";
        case ISerialization::Kind::DETACHED:
            return "Detached";
        case ISerialization::Kind::REPLICATED:
            return "Replicated";
    }
}

String ISerialization::kindStackToString(const KindStack & kind_stack)
{
    chassert(!kind_stack.empty() && kind_stack.front() == Kind::DEFAULT);
    /// For compatibility, names are formed like this:
    /// [Default] -> "Default"
    /// [Default, Kind1, Kind2, Kind3] -> Kind3OverKind2OverKind1
    String result;
    if (kind_stack.size() == 1)
        return kindToString(kind_stack.front());

    for (ssize_t i = kind_stack.size() - 1; i >= 1; i--)
    {
        if (!result.empty())
            result += "Over";
        result += kindToString(kind_stack[i]);
    }

    return result;
}

static ISerialization::Kind stringToKind(std::string_view str)
{
    if (str == "Default")
        return ISerialization::Kind::DEFAULT;
    else if (str == "Sparse")
        return ISerialization::Kind::SPARSE;
    else if (str == "Detached")
        return ISerialization::Kind::DETACHED;
    else if (str == "Replicated")
        return ISerialization::Kind::REPLICATED;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown serialization kind '{}'", str);
}

ISerialization::KindStack ISerialization::stringToKindStack(const String & str)
{
    std::vector<std::string_view> kind_strings = absl::StrSplit(str, absl::ByString("Over"));
    KindStack kind_stack;
    for (size_t i = 0; i != kind_strings.size(); ++i)
    {
        auto kind = stringToKind(kind_strings[i]);
        /// For compatibility we don't write first default kind in a chain of kinds.
        if (i == 0 && kind != Kind::DEFAULT)
            kind_stack.push_back(Kind::DEFAULT);
        kind_stack.push_back(kind);
    }

    return kind_stack;
}

bool ISerialization::hasKind(const KindStack & kind_stack, Kind kind)
{
    return std::find(kind_stack.begin(), kind_stack.end(), kind) != kind_stack.end();
}

const std::set<SubstreamType> ISerialization::Substream::named_types
{
    StringSizes,
    InlinedStringSizes,
    TupleElement,
    NamedOffsets,
    NamedNullMap,
    NamedVariantDiscriminators,
    QuantizedCodes,
    ProductQuantizationCodebook,
    MapKeyValue,
    ObjectDistinctPaths,
    ObjectSubObject,
    ObjectCombinedPath,
};

String ISerialization::Substream::toString() const
{
    if (named_types.contains(type))
        return fmt::format("{}({})", type, name_of_substream);

    if (type == VariantElement)
        return fmt::format("VariantElement({})", variant_element_name);

    if (type == VariantElementNullMap)
        return fmt::format("VariantElementNullMap({}.null)", variant_element_name);

    return String(magic_enum::enum_name(type));
}

String ISerialization::SubstreamPath::toString() const
{
    WriteBufferFromOwnString wb;
    wb << "{";
    for (size_t i = 0; i < size(); ++i)
    {
        if (i != 0)
            wb << ", ";
        wb << at(i).toString();
    }
    wb << "}";
    return wb.str();
}

void ISerialization::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::Regular);
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void ISerialization::enumerateStreams(
    const StreamCallback & callback,
    const DataTypePtr & type,
    const ColumnPtr & column) const
{
    EnumerateStreamsSettings settings;
    auto data = SubstreamData(getPtr()).withType(type).withColumn(column);
    enumerateStreams(settings, callback, data);
}

void ISerialization::enumerateAllStreams(
    const StreamCallback & callback,
    const DataTypePtr & type,
    const ColumnPtr & column) const
{
    EnumerateStreamsSettings settings;
    settings.enumerate_virtual_streams = true;
    auto data = SubstreamData(getPtr()).withType(type).withColumn(column);
    enumerateStreams(settings, callback, data);
}

void ISerialization::serializeBinaryBulk(const IColumn & column, WriteBuffer &, size_t, size_t) const
{
    throw Exception(ErrorCodes::MULTIPLE_STREAMS_REQUIRED, "Column {} must be serialized with multiple streams", column.getName());
}

void ISerialization::deserializeBinaryBulk(IColumn & column, ReadBuffer &, size_t, double) const
{
    throw Exception(ErrorCodes::MULTIPLE_STREAMS_REQUIRED, "Column {} must be deserialized with multiple streams", column.getName());
}

void ISerialization::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & /* state */) const
{
    settings.path.push_back(Substream::Regular);
    if (WriteBuffer * stream = settings.getter(settings.path))
        serializeBinaryBulk(column, *stream, offset, limit);
    settings.path.pop_back();
}

void ISerialization::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);

    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        /// Data was inserted from substreams cache.
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        size_t prev_size = column.size();
        double avg_value_size_hint = 0.0;
        if (settings.get_avg_value_size_hint_callback)
            avg_value_size_hint = settings.get_avg_value_size_hint_callback(settings.path);
        deserializeBinaryBulk(column, *stream, limit, avg_value_size_hint);
        size_t num_read_rows = column.size() - prev_size;
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings, column.getPtr(), num_read_rows);
        if (settings.update_avg_value_size_hint_callback)
            settings.update_avg_value_size_hint_callback(settings.path, column);
    }

    settings.path.pop_back();
}

namespace
{

using SubstreamIterator = ISerialization::SubstreamPath::const_iterator;

bool isPossibleOffsetsOfNested(const ISerialization::SubstreamPath & path)
{
    /// Arrays of Nested cannot be inside other types.
    /// So it's ok to check only first element of path.

    /// Array offsets as a part of serialization of Array type.
    if (path.size() == 1
        && path[0].type == ISerialization::Substream::ArraySizes)
        return true;

    /// Array offsets as a separate subcolumn.
    if (path.size() == 2
        && path[0].type == ISerialization::Substream::NamedOffsets
        && path[1].type == ISerialization::Substream::Regular
        && path[0].name_of_substream == "size0")
        return true;

    return false;
}

/// The escaped column name a stream name starts with, or the escaped Nested table name for the
/// offsets stream that the columns of a flattened Nested group share.
String getStreamNamePrefix(const String & name_in_storage, const ISerialization::SubstreamPath & path, bool share_nested_offsets)
{
    if (share_nested_offsets)
    {
        auto nested_storage_name = Nested::extractTableName(name_in_storage);
        if (name_in_storage != nested_storage_name && isPossibleOffsetsOfNested(path))
            return escapeForFileName(nested_storage_name);
    }

    return escapeForFileName(name_in_storage);
}

/// Flags that differ between the three renderings: file names, subcolumn names and cache keys.
struct NameRenderingSettings
{
    bool escape_for_file_name = false;
    bool encode_sparse_stream = false;
    bool escape_variant_substreams = false;
    bool namespaced = false;
};

/// Component this substream contributes to the name, empty when it is only a descent into a
/// container. `array_level` and `nullable_depth` include this substream.
String getComponentForSubstream(
    const ISerialization::Substream & substream, const NameRenderingSettings & settings, size_t array_level, size_t nullable_depth)
{
    using Substream = ISerialization::Substream;

    const auto type = substream.type;

    if (type == Substream::NullMap || type == Substream::SparseNullMap || type == Substream::NamedNullMap)
        return nullable_depth == 0 ? ".null" : ".null" + toString(nullable_depth);

    if (type == Substream::ArraySizes || type == Substream::NamedOffsets)
    {
        /// `ArrayElements` opens a new namespace right away, so at most one `ArraySizes` occurs per
        /// namespace and the level number is redundant.
        return settings.namespaced ? ".size" : ".size" + toString(array_level);
    }

    if (type == Substream::ArrayElements)
        return settings.namespaced ? ".arr_elems" : "";

    if (type == Substream::NullableElements)
        return settings.namespaced ? ".null_elems" : "";

    if (type == Substream::ObjectPaths)
        return settings.namespaced ? ".object_paths" : "";

    if (type == Substream::StringSizes || type == Substream::InlinedStringSizes)
        return ".size";
    if (type == Substream::DictionaryKeys)
        return ".dict";
    if (type == Substream::DictionaryKeysPrefix)
        return ".dict_prefix";
    if (type == Substream::SparseElements)
        return settings.encode_sparse_stream ? ".sparse" : "";
    if (type == Substream::SparseOffsets)
        return ".sparse.idx";
    if (type == Substream::ReplicatedElements)
        return ".repl";
    if (type == Substream::ReplicatedIndexes)
        return ".repl.idx";

    if (Substream::named_types.contains(type))
    {
        /// For compatibility reasons, we use %2E (escaped dot) instead of dot.
        /// Because nested data may be represented not by Array of Tuple,
        /// but by separate Array columns with names in a form of a.b,
        /// and name is encoded as a whole.
        /// NAMESPACED uses a uniform separator, which makes the component sequence decodable.
        if (type == Substream::TupleElement && settings.escape_for_file_name)
        {
            if (settings.namespaced)
                return "." + escapeForFileName(substream.name_of_substream);
            return escapeForFileName("." + substream.name_of_substream);
        }
        return "." + substream.name_of_substream;
    }

    if (type == Substream::VariantDiscriminators)
        return ".variant_discr";
    if (type == Substream::VariantDiscriminatorsPrefix)
        return ".variant_discr_prefix";
    if (type == Substream::VariantOffsets)
        return ".variant_offsets";
    if (type == Substream::VariantElement || type == Substream::VariantElementNullMap)
    {
        auto name = settings.escape_for_file_name && settings.escape_variant_substreams
            ? "." + escapeForFileName(substream.variant_element_name)
            : "." + substream.variant_element_name;
        return type == Substream::VariantElementNullMap ? name + ".null" : name;
    }

    if (type == SubstreamType::DynamicStructure)
        return ".dynamic_structure";
    if (type == SubstreamType::ObjectStructure)
        return ".object_structure";
    if (type == SubstreamType::ObjectSharedData)
        return ".object_shared_data";
    if (type == SubstreamType::Bucket)
        return "." + std::to_string(substream.bucket);
    if (type == SubstreamType::MapBucketsInfo)
        return ".buckets_info";
    if (type == SubstreamType::MapBucketIndexes)
        return ".bucket_indexes";
    if (type == SubstreamType::ObjectSharedDataStructure)
        return ".structure";
    if (type == SubstreamType::ObjectSharedDataStructurePrefix)
        return ".structure_prefix";
    if (type == SubstreamType::ObjectSharedDataStructureSuffix)
        return ".structure_suffix";
    if (type == SubstreamType::ObjectSharedDataSubstreams)
        return ".substreams";
    if (type == SubstreamType::ObjectSharedDataPathsMarks)
        return ".paths_marks";
    if (type == SubstreamType::ObjectSharedDataSubstreamsMarks)
        return ".substreams_marks";
    if (type == SubstreamType::ObjectSharedDataPathsSubstreamsMetadata)
        return ".paths_substreams_metadata";
    if (type == SubstreamType::ObjectSharedDataPathsInfos)
        return ".paths_infos";
    if (type == SubstreamType::ObjectSharedDataData)
        return ".data";
    if (type == SubstreamType::ObjectSharedDataCopy)
        return ".copy";
    if (type == SubstreamType::ObjectSharedDataCopySizes)
        return ".sizes";
    if (type == SubstreamType::ObjectSharedDataCopyPathsIndexes)
        return ".paths_indexes";
    if (type == SubstreamType::ObjectSharedDataCopyValues)
        return ".values";
    if (type == SubstreamType::ObjectTypedPath || type == SubstreamType::ObjectDynamicPath)
        return "." + (settings.escape_for_file_name ? escapeForFileName(substream.object_path_name) : substream.object_path_name);

    return "";
}

String getNameForSubstreamPath(
    String stream_name,
    SubstreamIterator begin,
    SubstreamIterator end,
    const NameRenderingSettings & settings,
    size_t initial_array_level = 0)
{
    using Substream = ISerialization::Substream;

    size_t array_level = initial_array_level;
    /// Counts `Nullable` levels that no component separates from the null map yet. Under NAMESPACED
    /// the containers contribute components too, which is why a file name never carries `.nullN`.
    size_t nullable_depth = 0;

    for (auto it = begin; it != end; ++it)
    {
        if (it->type == Substream::ArrayElements)
            ++array_level;
        else if (it->type == Substream::NullableElements)
            ++nullable_depth;

        auto component = getComponentForSubstream(*it, settings, array_level, nullable_depth);
        if (!component.empty())
            nullable_depth = 0;

        stream_name += component;
    }

    return stream_name;
}

}

String ISerialization::getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path, const StreamFileNameSettings & settings)
{
    return getFileNameForStream(column.getNameInStorage(), path, settings);
}

String ISerialization::getFileNameForStream(const String & name_in_storage, const SubstreamPath & path, const StreamFileNameSettings & settings)
{
    return getNameForSubstreamPath(
        getStreamNamePrefix(name_in_storage, path, settings.share_nested_offsets),
        path.begin(),
        path.end(),
        {.escape_for_file_name = true,
         .escape_variant_substreams = settings.escape_variant_substreams,
         .namespaced = settings.substream_naming_version == MergeTreeSubstreamNamingVersion::NAMESPACED});
}

String ISerialization::getFileNameForRenamedColumnStream(const String & name_from, const String & name_to, const String & file_name)
{
    auto name_from_escaped = escapeForFileName(name_from);
    if (file_name.starts_with(name_from_escaped))
        return escapeForFileName(name_to) + file_name.substr(name_from_escaped.size());

    auto nested_storage_name_escaped = escapeForFileName(Nested::extractTableName(name_from));
    if (file_name.starts_with(nested_storage_name_escaped))
        return escapeForFileName(Nested::extractTableName(name_to)) + file_name.substr(nested_storage_name_escaped.size());

    throw Exception(ErrorCodes::LOGICAL_ERROR, "File name {} doesn't correspond to column {}", file_name, name_from);
}

String ISerialization::getFileNameForRenamedColumnStream(const NameAndTypePair & column_from, const NameAndTypePair & column_to, const String & file_name)
{
    return getFileNameForRenamedColumnStream(column_from.getNameInStorage(), column_to.getNameInStorage(), file_name);
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path)
{
    return getSubcolumnNameForStream(path, path.size());
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len, size_t initial_array_level)
{
    /// Frozen for compatibility: flat, unescaped and never namespaced.
    auto subcolumn_name = getNameForSubstreamPath("", path.begin(), path.begin() + prefix_len, {}, initial_array_level);
    if (!subcolumn_name.empty())
        subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.

    return subcolumn_name;
}

String ISerialization::getSubstreamsCacheKeyForStream(const String & name_in_storage, const SubstreamPath & path, bool share_nested_offsets)
{
    /// The subcolumn name is not injective (two streams of one column can share it while their files
    /// differ, e.g. `c.size0` and `c%2Esize0` for Array(Tuple(`size0` UInt64))), so the file name
    /// rendering is used. The scheme is always NAMESPACED regardless of the part: the key never touches
    /// disk, and only that form is injective for every type.
    return getNameForSubstreamPath(
        getStreamNamePrefix(name_in_storage, path, share_nested_offsets),
        path.begin(),
        path.end(),
        {.escape_for_file_name = true, .encode_sparse_stream = true, .escape_variant_substreams = true, .namespaced = true});
}

namespace
{

/// Element of substeams cache that contains single column and number of read rows from current range
/// (it might be different from column size as single column can contain rows from multiple ranges).
struct SubstreamsCacheColumnWithNumReadRowsElement : public ISerialization::ISubstreamsCacheElement
{
    explicit SubstreamsCacheColumnWithNumReadRowsElement(ColumnPtr column_, size_t num_read_rows_) : column(column_), num_read_rows(num_read_rows_) {}

    ColumnPtr column;
    size_t num_read_rows;
};

}

void ISerialization::addColumnWithNumReadRowsToSubstreamsCache(
    SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings, ColumnPtr column, size_t num_read_rows)
{
    /// The consumers of this cache element insert the last num_read_rows rows of the column into the
    /// result (see insertDataFromCachedColumn), so the column must contain at least that many rows,
    /// otherwise the range arithmetic there would underflow.
    chassert(column);
    chassert(column->size() >= num_read_rows);
    addElementToSubstreamsCache(cache, settings, std::make_unique<SubstreamsCacheColumnWithNumReadRowsElement>(column, num_read_rows));
}

std::optional<std::pair<ColumnPtr, size_t>>
ISerialization::getColumnWithNumReadRowsFromSubstreamsCache(SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings)
{
    auto * element = getElementFromSubstreamsCache(cache, settings);
    if (!element)
        return std::nullopt;

    auto * typed_element = assert_cast<SubstreamsCacheColumnWithNumReadRowsElement *>(element);
    /// The invariant established at insertion must still hold at lookup. If it does not, the cached
    /// column was mutated in place through another reference after it was cached (broken copy-on-write
    /// discipline, see https://github.com/ClickHouse/ClickHouse/issues/105626).
    chassert(typed_element->column);
    chassert(typed_element->column->size() >= typed_element->num_read_rows);
    return std::make_pair(typed_element->column, typed_element->num_read_rows);
}

void ISerialization::addElementToSubstreamsCache(
    ISerialization::SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings, std::unique_ptr<ISubstreamsCacheElement> && element)
{
    addElementToSubstreamsCache(cache, settings, settings.path, std::move(element));
}

void ISerialization::addElementToSubstreamsCache(
    ISerialization::SubstreamsCache * cache,
    const DeserializeBinaryBulkSettings & settings,
    const SubstreamPath & path,
    std::unique_ptr<ISubstreamsCacheElement> && element)
{
    if (!cache)
        return;

    cache->insert_or_assign(
        getSubstreamsCacheKeyForStream(settings.name_in_storage, path, settings.share_nested_offsets), std::move(element));
}

ISerialization::ISubstreamsCacheElement * ISerialization::getElementFromSubstreamsCache(
    ISerialization::SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings)
{
    return getElementFromSubstreamsCache(cache, settings, settings.path);
}

ISerialization::ISubstreamsCacheElement * ISerialization::getElementFromSubstreamsCache(
    ISerialization::SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings, const SubstreamPath & path)
{
    if (!cache)
        return nullptr;

    auto it = cache->find(getSubstreamsCacheKeyForStream(settings.name_in_storage, path, settings.share_nested_offsets));
    return it == cache->end() ? nullptr : it->second.get();
}

void ISerialization::addToSubstreamsDeserializeStatesCache(
    SubstreamsDeserializeStatesCache * cache, const DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr state)
{
    if (!cache)
        return;

    cache->emplace(getSubstreamsCacheKeyForStream(settings.name_in_storage, settings.path, settings.share_nested_offsets), state);
}

ISerialization::DeserializeBinaryBulkStatePtr ISerialization::getFromSubstreamsDeserializeStatesCache(
    SubstreamsDeserializeStatesCache * cache, const DeserializeBinaryBulkSettings & settings)
{
    return getFromSubstreamsDeserializeStatesCache(cache, settings, settings.path);
}

ISerialization::DeserializeBinaryBulkStatePtr ISerialization::getFromSubstreamsDeserializeStatesCache(
    SubstreamsDeserializeStatesCache * cache, const DeserializeBinaryBulkSettings & settings, const SubstreamPath & path)
{
    if (!cache)
        return nullptr;

    auto it = cache->find(getSubstreamsCacheKeyForStream(settings.name_in_storage, path, settings.share_nested_offsets));
    return it == cache->end() ? nullptr : it->second;
}

bool ISerialization::isSpecialCompressionAllowed(const SubstreamPath & path)
{
    for (const auto & elem : path)
    {
        if (elem.type == Substream::NullMap
            || elem.type == Substream::ArraySizes
            || elem.type == Substream::StringSizes
            || elem.type == Substream::DictionaryIndexes
            || elem.type == Substream::SparseOffsets)
            return false;
    }
    return true;
}

namespace
{

template <typename F>
bool tryDeserializeText(const F deserialize, DB::IColumn & column)
{
    size_t prev_size = column.size();
    try
    {
        deserialize(column);
        return true;
    }
    catch (...) // Ok: tryDeserializeText is a try-pattern
    {
        if (column.size() > prev_size)
            column.popBack(column.size() - prev_size);
        rethrowIfNotParseError();
        return false;
    }
}

}

void ISerialization::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeBinary(column, row_num, ostr, {});
}

void ISerialization::serializeTextHive(const IColumn & /*column*/, size_t /*row_num*/, WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeTextHive is not implemented for this type");
}

char getHiveTextDelimiter(const FormatSettings & settings, size_t nesting_level)
{
    /// Apache Hive's LazySimpleSerDe uses a fixed list of separators indexed by nesting depth.
    /// The first three are the configurable field, collection-items and map-keys delimiters; the
    /// deeper ones default to consecutive control characters (0x04, 0x05, ..., 0x08). See
    /// org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters.
    switch (nesting_level)
    {
        case 0:
            return settings.hive_text.fields_delimiter;
        case 1:
            return settings.hive_text.collection_items_delimiter;
        case 2:
            return settings.hive_text.map_keys_delimiter;
        default:
            if (nesting_level <= 7)
                return static_cast<char>(nesting_level + 1);
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The data is nested too deeply for the HiveText output format, which supports at "
                "most 8 nesting levels of separators (matching Apache Hive's LazySimpleSerDe)");
    }
}

bool ISerialization::tryDeserializeTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    return tryDeserializeText([&](DB::IColumn & my_column) { deserializeTextCSV(my_column, istr, settings); }, column);
}

bool ISerialization::tryDeserializeTextEscaped(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    return tryDeserializeText([&](DB::IColumn & my_column) { deserializeTextEscaped(my_column, istr, settings); }, column);
}

bool ISerialization::tryDeserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    return tryDeserializeText([&](DB::IColumn & my_column) { deserializeTextJSON(my_column, istr, settings); }, column);
}

bool ISerialization::tryDeserializeTextQuoted(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    return tryDeserializeText([&](DB::IColumn & my_column) { deserializeTextQuoted(my_column, istr, settings); }, column);
}

bool ISerialization::tryDeserializeWholeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    return tryDeserializeText([&](DB::IColumn & my_column) { deserializeWholeText(my_column, istr, settings); }, column);
}

void ISerialization::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    /// Read until \t or \n.
    readString(field, istr);
    ReadBufferFromString buf(field);
    deserializeWholeText(column, buf, settings);
}

bool ISerialization::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    /// Read until \t or \n.
    readString(field, istr);
    ReadBufferFromString buf(field);
    return tryDeserializeWholeText(column, buf, settings);
}

void ISerialization::serializeTextMarkdown(
    const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings & settings) const
{
    serializeTextEscaped(column, row_num, ostr, settings);
}

void ISerialization::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

size_t ISerialization::getArrayLevel(const SubstreamPath & path, size_t prefix_len)
{
    size_t level = 0;
    for (size_t i = 0; i < prefix_len; ++i)
        level += path[i].type == Substream::ArrayElements;
    return level;
}

bool ISerialization::hasSubcolumnForPath(const SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    size_t last_elem = prefix_len - 1;
    return path[last_elem].type == Substream::NullMap
            || path[last_elem].type == Substream::SparseNullMap
            || path[last_elem].type == Substream::TupleElement
            || path[last_elem].type == Substream::ArraySizes
            || path[last_elem].type == Substream::StringSizes
            || path[last_elem].type == Substream::InlinedStringSizes
            || path[last_elem].type == Substream::VariantElement
            || path[last_elem].type == Substream::VariantElementNullMap
            || path[last_elem].type == Substream::ObjectTypedPath
            || path[last_elem].type == Substream::QuantizedCodes
            || path[last_elem].type == Substream::ProductQuantizationCodebook;
}

bool ISerialization::isDeclaredSubstream(const SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    auto type = path[prefix_len - 1].type;
    return type == Substream::TupleElement || type == Substream::ObjectTypedPath;
}

bool ISerialization::isEphemeralSubcolumn(const DB::ISerialization::SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    size_t last_elem = prefix_len - 1;
    return path[last_elem].type == Substream::VariantElementNullMap || path[last_elem].type == Substream::InlinedStringSizes
        || path[last_elem].type == Substream::SparseNullMap;
}

bool ISerialization::isPrefetchNeededForSubstream(const DB::ISerialization::SubstreamPath & path, size_t prefix_len, bool prefetch_json_shared_data_substreams)
{
    if (prefetch_json_shared_data_substreams || prefix_len == 0 || prefix_len > path.size())
        return true;

    /// The JSON shared data Data substream is not read from the start of the granule: a path's data is
    /// located via a mark in another stream and read by seeking to it. With many JSON paths the granule
    /// is large, so prefetching from the start can fetch data we never read.
    return path[prefix_len - 1].type != Substream::ObjectSharedDataData;
}

bool ISerialization::isDynamicSubcolumn(const DB::ISerialization::SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    for (size_t i = 0; i != prefix_len; ++i)
    {
        if (path[i].type == SubstreamType::DynamicData || path[i].type == SubstreamType::DynamicStructure
            || path[i].type == SubstreamType::ObjectPaths || path[i].type == SubstreamType::ObjectSharedData
            || path[i].type == SubstreamType::ObjectStructure)
            return true;
    }

    return false;
}

bool ISerialization::isLowCardinalityDictionarySubcolumn(const DB::ISerialization::SubstreamPath & path)
{
    if (path.empty())
        return false;

    return path[path.size() - 1].type == SubstreamType::DictionaryKeys;
}

bool ISerialization::isMetadataStream(const DB::ISerialization::SubstreamPath & path)
{
    if (path.empty())
        return false;

    return path[path.size() - 1].type == SubstreamType::DynamicStructure || path[path.size() - 1].type == SubstreamType::ObjectStructure
        || path[path.size() - 1].type == SubstreamType::MapBucketsInfo;
}

bool ISerialization::isSingleValuePerPartStream(const DB::ISerialization::SubstreamPath & path)
{
    /// The whole path is scanned rather than only its last element, because the codebook substream is terminal
    /// only when the column itself is enumerated; when the subcolumn is read on its own, the path of its stream
    /// is `ProductQuantizationCodebook` followed by the `Regular` substream of the underlying `FixedString`.
    for (const auto & elem : path)
        if (elem.type == SubstreamType::ProductQuantizationCodebook)
            return true;

    return false;
}

bool ISerialization::hasPrefix(const DB::ISerialization::SubstreamPath & path, bool use_specialized_prefixes_and_suffixes_substreams)
{
    if (path.empty())
        return false;

    switch (path[path.size() - 1].type)
    {
        case SubstreamType::DynamicStructure: [[fallthrough]];
        case SubstreamType::ObjectStructure: [[fallthrough]];
        case SubstreamType::DeprecatedObjectStructure: [[fallthrough]];
        case SubstreamType::DictionaryKeysPrefix: [[fallthrough]];
        case SubstreamType::VariantDiscriminatorsPrefix:
            return true;
        case SubstreamType::DictionaryKeys: [[fallthrough]];
        case SubstreamType::VariantDiscriminators:
            return !use_specialized_prefixes_and_suffixes_substreams;
        default:
            return false;
    }
}

ISerialization::SubstreamData ISerialization::createFromPath(const SubstreamPath & path, size_t prefix_len)
{
    chassert(prefix_len <= path.size());
    if (prefix_len == 0)
        return {};

    ssize_t last_elem = prefix_len - 1;
    auto res = path[last_elem].data;

    /// Materialize the column on demand via a lazy creator if one is attached.
    /// This supports deferred column creation for derived subcolumns like
    /// String `.size`, whose data is computed from a parent column and should
    /// only be materialized when actually requested.
    if (!res.column && res.lazy_column_creator)
        res.column = res.lazy_column_creator();

    for (ssize_t i = last_elem - 1; i >= 0; --i)
    {
        const auto & creator = path[i].creator;
        if (creator)
        {
            res.serialization = res.serialization ? creator->create(res.serialization, res.type) : res.serialization;
            res.type = res.type ? creator->create(res.type) : res.type;
            res.column = res.column ? creator->create(res.column) : res.column;
        }
    }

    return res;
}

void ISerialization::throwUnexpectedDataAfterParsedValue(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const String & type_name) const
{
    WriteBufferFromOwnString ostr;
    serializeText(column, column.size() - 1, ostr, settings);
    /// Restore correct column size.
    column.popBack(1);
    throw Exception(
        ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
        "Unexpected data '{}' after parsed {} value '{}'",
        std::string(istr.position(), std::min(size_t(10), istr.available())),
        type_name,
        ostr.str());
}

ISerialization::StreamFileNameSettings::StreamFileNameSettings(
    const MergeTreeSettings & merge_tree_settings, const SerializationInfoSettings * info_settings)
{
    escape_variant_substreams = merge_tree_settings[MergeTreeSetting::escape_variant_subcolumn_filenames];
    share_nested_offsets = merge_tree_settings[MergeTreeSetting::share_nested_offsets];
    substream_naming_version = info_settings
        ? info_settings->substream_naming_version
        : merge_tree_settings[MergeTreeSetting::substream_naming_version];
}

void ISerialization::addSubstreamAndCallCallback(ISerialization::SubstreamPath & path, const ISerialization::StreamCallback & callback, ISerialization::Substream substream) const
{
    path.push_back(substream);
    callback(path);
    path.pop_back();
}

bool ISerialization::insertDataFromSubstreamsCacheIfAny(SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings, IColumn & result_column)
{
    auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings);
    if (!cached_column_with_num_read_rows)
        return false;

    insertDataFromCachedColumn(result_column, cached_column_with_num_read_rows->first, cached_column_with_num_read_rows->second);
    return true;
}

void ISerialization::insertDataFromCachedColumn(IColumn & result_column, const ColumnPtr & cached_column, size_t num_read_rows)
{
    /// Copy only the current range out of the cached column; consumers never adopt the cached pointer,
    /// so each reader keeps its own column (no COW clone needed). result_column must differ from the
    /// cached column, otherwise this is a self-insert whose source can be invalidated mid-copy.
    chassert(&result_column != cached_column.get());
    /// The range arithmetic relies on this invariant, otherwise `cached_column->size() - num_read_rows` underflows.
    chassert(cached_column->size() >= num_read_rows);
    result_column.insertRangeFrom(*cached_column, cached_column->size() - num_read_rows, num_read_rows);
}

bool ISerialization::isVariantSubcolumn(const SubstreamPath & substream_path)
{
    for (const auto & stream : substream_path)
    {
        if (stream.type == Substream::VariantElement)
            return true;
    }

    return false;
}

bool ISerialization::tryToChangeStreamFileNameSettingsForNotFoundStream(const ISerialization::SubstreamPath & substream_path, ISerialization::StreamFileNameSettings & stream_file_name_settings)
{
    if (isVariantSubcolumn(substream_path))
    {
        stream_file_name_settings.escape_variant_substreams = !stream_file_name_settings.escape_variant_substreams;
        return true;
    }

    return false;
}

}
