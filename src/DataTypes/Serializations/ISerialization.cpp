#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Common/Exception.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/EnumReflection.h>
#include <base/demangle.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/assert_cast.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <boost/algorithm/string_regex.hpp>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool escape_variant_subcolumn_filenames;
}

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int LOGICAL_ERROR;
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

static ISerialization::Kind stringToKind(const String & str)
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
    std::vector<String> kind_strings;
    boost::algorithm::split_regex(kind_strings, str, boost::regex("Over"));
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

void ISerialization::deserializeBinaryBulk(IColumn & column, ReadBuffer &, size_t, size_t, double) const
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
    ColumnPtr & column,
    size_t rows_offset,
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
        size_t prev_size = column->size();
        auto mutable_column = column->assumeMutable();
        double avg_value_size_hint = 0.0;
        if (settings.get_avg_value_size_hint_callback)
            avg_value_size_hint = settings.get_avg_value_size_hint_callback(settings.path);
        deserializeBinaryBulk(*mutable_column, *stream, rows_offset, limit, avg_value_size_hint);
        column = std::move(mutable_column);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
        if (settings.update_avg_value_size_hint_callback)
            settings.update_avg_value_size_hint_callback(settings.path, *column);
    }

    settings.path.pop_back();
}

namespace
{

using SubstreamIterator = ISerialization::SubstreamPath::const_iterator;

String getNameForSubstreamPath(
    String stream_name,
    SubstreamIterator begin,
    SubstreamIterator end,
    bool escape_for_file_name,
    bool encode_sparse_stream,
    bool escape_variant_substreams,
    size_t initial_array_level = 0)
{
    using Substream = ISerialization::Substream;

    size_t array_level = initial_array_level;
    for (auto it = begin; it != end; ++it)
    {
        if (it->type == Substream::NullMap || it->type == Substream::SparseNullMap)
            stream_name += ".null";
        else if (it->type == Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (it->type == Substream::ArrayElements)
            ++array_level;
        else if (it->type == Substream::StringSizes || it->type == Substream::InlinedStringSizes)
            stream_name += ".size";
        else if (it->type == Substream::DictionaryKeys)
            stream_name += ".dict";
        else if (it->type == Substream::DictionaryKeysPrefix)
            stream_name += ".dict_prefix";
        else if (it->type == Substream::SparseElements && encode_sparse_stream)
            stream_name += ".sparse";
        else if (it->type == Substream::SparseOffsets)
            stream_name += ".sparse.idx";
        else if (it->type == Substream::ReplicatedElements)
            stream_name += ".repl";
        else if (it->type == Substream::ReplicatedIndexes)
            stream_name += ".repl.idx";
        else if (Substream::named_types.contains(it->type))
        {
            auto substream_name = "." + it->name_of_substream;

            /// For compatibility reasons, we use %2E (escaped dot) instead of dot.
            /// Because nested data may be represented not by Array of Tuple,
            /// but by separate Array columns with names in a form of a.b,
            /// and name is encoded as a whole.
            if (it->type == Substream::TupleElement && escape_for_file_name)
                stream_name += escapeForFileName(substream_name);
            else
                stream_name += substream_name;
        }
        else if (it->type == Substream::VariantDiscriminators)
            stream_name += ".variant_discr";
        else if (it->type == Substream::VariantDiscriminatorsPrefix)
            stream_name += ".variant_discr_prefix";
        else if (it->type == Substream::VariantOffsets)
            stream_name += ".variant_offsets";
        else if (it->type == Substream::VariantElement)
        {
            if (escape_for_file_name && escape_variant_substreams)
                stream_name += "." + escapeForFileName(it->variant_element_name);
            else
                stream_name += "." + it->variant_element_name;
        }
        else if (it->type == Substream::VariantElementNullMap)
        {
            if (escape_for_file_name && escape_variant_substreams)
                stream_name += "." + escapeForFileName(it->variant_element_name) + ".null";
            else
                stream_name += "." + it->variant_element_name + ".null";
        }
        else if (it->type == SubstreamType::DynamicStructure)
            stream_name += ".dynamic_structure";
        else if (it->type == SubstreamType::ObjectStructure)
            stream_name += ".object_structure";
        else if (it->type == SubstreamType::ObjectSharedData)
            stream_name += ".object_shared_data";
        else if (it->type == SubstreamType::ObjectSharedDataBucket)
            stream_name +=   "." + std::to_string(it->object_shared_data_bucket);
        else if (it->type == SubstreamType::ObjectSharedDataStructure)
            stream_name += ".structure";
        else if (it->type == SubstreamType::ObjectSharedDataStructurePrefix)
            stream_name += ".structure_prefix";
        else if (it->type == SubstreamType::ObjectSharedDataStructureSuffix)
            stream_name += ".structure_suffix";
        else if (it->type == SubstreamType::ObjectSharedDataSubstreams)
            stream_name += ".substreams";
        else if (it->type == SubstreamType::ObjectSharedDataPathsMarks)
            stream_name += ".paths_marks";
        else if (it->type == SubstreamType::ObjectSharedDataSubstreamsMarks)
            stream_name += ".substreams_marks";
        else if (it->type == SubstreamType::ObjectSharedDataPathsSubstreamsMetadata)
            stream_name += ".paths_substreams_metadata";
        else if (it->type == SubstreamType::ObjectSharedDataPathsInfos)
            stream_name += ".paths_infos";
        else if (it->type == SubstreamType::ObjectSharedDataData)
            stream_name += ".data";
        else if (it->type == SubstreamType::ObjectSharedDataCopy)
            stream_name += ".copy";
        else if (it->type == SubstreamType::ObjectSharedDataCopySizes)
            stream_name += ".sizes";
        else if (it->type == SubstreamType::ObjectSharedDataCopyPathsIndexes)
            stream_name += ".paths_indexes";
        else if (it->type == SubstreamType::ObjectSharedDataCopyValues)
            stream_name += ".values";
        else if (it->type == SubstreamType::ObjectTypedPath || it->type == SubstreamType::ObjectDynamicPath)
            stream_name += "." + (escape_for_file_name ? escapeForFileName(it->object_path_name) : it->object_path_name);
    }

    return stream_name;
}

}

String ISerialization::getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path, const StreamFileNameSettings & settings)
{
    return getFileNameForStream(column.getNameInStorage(), path, settings);
}

static bool isPossibleOffsetsOfNested(const ISerialization::SubstreamPath & path)
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

String ISerialization::getFileNameForStream(const String & name_in_storage, const SubstreamPath & path, const StreamFileNameSettings & settings)
{
    String stream_name;
    auto nested_storage_name = Nested::extractTableName(name_in_storage);
    if (name_in_storage != nested_storage_name && isPossibleOffsetsOfNested(path))
        stream_name = escapeForFileName(nested_storage_name);
    else
        stream_name = escapeForFileName(name_in_storage);

    return getNameForSubstreamPath(std::move(stream_name), path.begin(), path.end(), true, false, settings.escape_variant_substreams);
}

String ISerialization::getFileNameForRenamedColumnStream(const String & name_from, const String & name_to, const String & file_name)
{
    auto name_from_escaped = escapeForFileName(name_from);
    if (file_name.starts_with(name_from_escaped))
        return escapeForFileName(name_to) + file_name.substr(0, name_from_escaped.size());

    auto nested_storage_name_escaped = escapeForFileName(Nested::extractTableName(name_from));
    if (file_name.starts_with(nested_storage_name_escaped))
        return escapeForFileName(Nested::extractTableName(name_to)) + file_name.substr(0, nested_storage_name_escaped.size());

    throw Exception(ErrorCodes::LOGICAL_ERROR, "File name {} doesn't correspond to column {}", file_name, name_from);
}

String ISerialization::getFileNameForRenamedColumnStream(const NameAndTypePair & column_from, const NameAndTypePair & column_to, const String & file_name)
{
    return getFileNameForRenamedColumnStream(column_from.getNameInStorage(), column_to.getNameInStorage(), file_name);
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path, bool encode_sparse_stream, size_t initial_array_level)
{
    return getSubcolumnNameForStream(path, path.size(), encode_sparse_stream, initial_array_level);
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len, bool encode_sparse_stream, size_t initial_array_level)
{
    auto subcolumn_name = getNameForSubstreamPath("", path.begin(), path.begin() + prefix_len, false, encode_sparse_stream, false, initial_array_level);
    if (!subcolumn_name.empty())
        subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.

    return subcolumn_name;
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

void ISerialization::addColumnWithNumReadRowsToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column, size_t num_read_rows)
{
    addElementToSubstreamsCache(cache, path, std::make_unique<SubstreamsCacheColumnWithNumReadRowsElement>(column, num_read_rows));
}

std::optional<std::pair<ColumnPtr, size_t>> ISerialization::getColumnWithNumReadRowsFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path)
{
    auto * element = getElementFromSubstreamsCache(cache, path);
    if (!element)
        return std::nullopt;

    auto * typed_element = assert_cast<SubstreamsCacheColumnWithNumReadRowsElement *>(element);
    return std::make_pair(typed_element->column, typed_element->num_read_rows);
}

void ISerialization::addElementToSubstreamsCache(ISerialization::SubstreamsCache * cache, const ISerialization::SubstreamPath & path, std::unique_ptr<ISubstreamsCacheElement> && element)
{
    if (!cache || path.empty())
        return;

    cache->insert_or_assign(getSubcolumnNameForStream(path, true), std::move(element));
}

ISerialization::ISubstreamsCacheElement * ISerialization::getElementFromSubstreamsCache(ISerialization::SubstreamsCache * cache, const ISerialization::SubstreamPath & path)
{
    if (!cache || path.empty())
        return nullptr;

    auto it = cache->find(getSubcolumnNameForStream(path, true));
    return it == cache->end() ? nullptr : it->second.get();
}

void ISerialization::addToSubstreamsDeserializeStatesCache(SubstreamsDeserializeStatesCache * cache, const SubstreamPath & path, DeserializeBinaryBulkStatePtr state)
{
    if (!cache || path.empty())
        return;

    cache->emplace(getSubcolumnNameForStream(path, true), state);
}

ISerialization::DeserializeBinaryBulkStatePtr ISerialization::getFromSubstreamsDeserializeStatesCache(SubstreamsDeserializeStatesCache * cache, const SubstreamPath & path)
{
    if (!cache || path.empty())
        return nullptr;

    auto it = cache->find(getSubcolumnNameForStream(path, true));
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
    catch (...)
    {
        if (column.size() > prev_size)
            column.popBack(column.size() - prev_size);
        return false;
    }
}

}

void ISerialization::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeBinary(column, row_num, ostr, {});
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
            || path[last_elem].type == Substream::ObjectTypedPath;
}

bool ISerialization::isEphemeralSubcolumn(const DB::ISerialization::SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    size_t last_elem = prefix_len - 1;
    return path[last_elem].type == Substream::VariantElementNullMap || path[last_elem].type == Substream::InlinedStringSizes
        || path[last_elem].type == Substream::SparseNullMap;
}

bool ISerialization::isDynamicSubcolumn(const DB::ISerialization::SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    for (size_t i = 0; i != prefix_len; ++i)
    {
        if (path[i].type == SubstreamType::DynamicData || path[i].type == SubstreamType::DynamicStructure
            || path[i].type == SubstreamType::ObjectData || path[i].type == SubstreamType::ObjectStructure)
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

bool ISerialization::isDynamicOrObjectStructureSubcolumn(const DB::ISerialization::SubstreamPath & path)
{
    if (path.empty())
        return false;

    return path[path.size() - 1].type == SubstreamType::DynamicStructure || path[path.size() - 1].type == SubstreamType::ObjectStructure;
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
    assert(prefix_len <= path.size());
    if (prefix_len == 0)
        return {};

    ssize_t last_elem = prefix_len - 1;
    auto res = path[last_elem].data;
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

ISerialization::StreamFileNameSettings::StreamFileNameSettings(const MergeTreeSettings & merge_tree_settings)
{
    escape_variant_substreams = merge_tree_settings[MergeTreeSetting::escape_variant_subcolumn_filenames];
}

void ISerialization::addSubstreamAndCallCallback(ISerialization::SubstreamPath & path, const ISerialization::StreamCallback & callback, ISerialization::Substream substream) const
{
    path.push_back(substream);
    callback(path);
    path.pop_back();
}

bool ISerialization::insertDataFromSubstreamsCacheIfAny(SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings, ColumnPtr & result_column)
{
    auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path);
    if (!cached_column_with_num_read_rows)
        return false;

    insertDataFromCachedColumn(settings, result_column, cached_column_with_num_read_rows->first, cached_column_with_num_read_rows->second, cache, /*update_cache_after_insert=*/ true);
    return true;
}

void ISerialization::insertDataFromCachedColumn(const ISerialization::DeserializeBinaryBulkSettings & settings, ColumnPtr & result_column, const ColumnPtr & cached_column, size_t num_read_rows, SubstreamsCache * cache, bool update_cache_after_insert)
{
    /// Usually substreams cache contains the whole column from currently deserialized block with rows from multiple ranges.
    /// It's done to avoid extra data copy, in this case we just use this cached column as the result column.
    /// But sometimes in cache we might have column with rows from the current range only (for example when we don't store this column but need it for
    /// constructing another column). In this case we need to insert data into resulting column from cached column.
    /// To determine what case we have we store number of read rows in last range in cache.
    if ((settings.insert_only_rows_in_current_range_from_substreams_cache) || (result_column != cached_column && !result_column->empty() && cached_column->size() == num_read_rows))
    {
        result_column->assumeMutable()->insertRangeFrom(*cached_column, cached_column->size() - num_read_rows, num_read_rows);
        if (update_cache_after_insert)
        {
            /// Replace column in the cache with the new column to avoid inserting into it again
            /// from currently cached range if this substream will be read again in current range.
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, result_column, num_read_rows);
        }
    }
    else
    {
        result_column = cached_column;
    }
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
    if (isVariantSubcolumn(substream_path) && stream_file_name_settings.escape_variant_substreams)
    {
        stream_file_name_settings.escape_variant_substreams = false;
        return true;
    }

    return false;
}

}
