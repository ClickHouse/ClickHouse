#include <DataTypes/Serializations/ISerialization.h>
#include <Compression/CompressionFactory.h>
#include <Columns/IColumn.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/NestedUtils.h>
#include <base/EnumReflection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int LOGICAL_ERROR;
}

ISerialization::Kind ISerialization::getKind(const IColumn & column)
{
    if (column.isSparse())
        return Kind::SPARSE;

    return Kind::DEFAULT;
}

String ISerialization::kindToString(Kind kind)
{
    switch (kind)
    {
        case Kind::DEFAULT:
            return "Default";
        case Kind::SPARSE:
            return "Sparse";
    }
    UNREACHABLE();
}

ISerialization::Kind ISerialization::stringToKind(const String & str)
{
    if (str == "Default")
        return Kind::DEFAULT;
    else if (str == "Sparse")
        return Kind::SPARSE;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown serialization kind '{}'", str);
}

const std::set<SubstreamType> ISerialization::Substream::named_types
{
    TupleElement,
    NamedOffsets,
    NamedNullMap,
};

String ISerialization::Substream::toString() const
{
    if (named_types.contains(type))
        return fmt::format("{}({})", type, name_of_substream);

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
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);

    auto cached_column = getFromSubstreamsCache(cache, settings.path);
    if (cached_column)
    {
        column = cached_column;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_column = column->assumeMutable();
        deserializeBinaryBulk(*mutable_column, *stream, limit, settings.avg_value_size_hint);
        column = std::move(mutable_column);
        addToSubstreamsCache(cache, settings.path, column);
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
    bool escape_tuple_delimiter)
{
    using Substream = ISerialization::Substream;

    size_t array_level = 0;
    for (auto it = begin; it != end; ++it)
    {
        if (it->type == Substream::NullMap)
            stream_name += ".null";
        else if (it->type == Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (it->type == Substream::ArrayElements)
            ++array_level;
        else if (it->type == Substream::DictionaryKeys)
            stream_name += ".dict";
        else if (it->type == Substream::SparseOffsets)
            stream_name += ".sparse.idx";
        else if (Substream::named_types.contains(it->type))
        {
            auto substream_name = "." + it->name_of_substream;

            /// For compatibility reasons, we use %2E (escaped dot) instead of dot.
            /// Because nested data may be represented not by Array of Tuple,
            /// but by separate Array columns with names in a form of a.b,
            /// and name is encoded as a whole.
            if (it->type == Substream::TupleElement && escape_tuple_delimiter)
                stream_name += escapeForFileName(substream_name);
            else
                stream_name += substream_name;
        }
    }

    return stream_name;
}

}

String ISerialization::getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path)
{
    return getFileNameForStream(column.getNameInStorage(), path);
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

String ISerialization::getFileNameForStream(const String & name_in_storage, const SubstreamPath & path)
{
    String stream_name;
    auto nested_storage_name = Nested::extractTableName(name_in_storage);
    if (name_in_storage != nested_storage_name && isPossibleOffsetsOfNested(path))
        stream_name = escapeForFileName(nested_storage_name);
    else
        stream_name = escapeForFileName(name_in_storage);

    return getNameForSubstreamPath(std::move(stream_name), path.begin(), path.end(), true);
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path)
{
    return getSubcolumnNameForStream(path, path.size());
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len)
{
    auto subcolumn_name = getNameForSubstreamPath("", path.begin(), path.begin() + prefix_len, false);
    if (!subcolumn_name.empty())
        subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.

    return subcolumn_name;
}

void ISerialization::addToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column)
{
    if (!cache || path.empty())
        return;

    cache->emplace(getSubcolumnNameForStream(path), column);
}

ColumnPtr ISerialization::getFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path)
{
    if (!cache || path.empty())
        return nullptr;

    auto it = cache->find(getSubcolumnNameForStream(path));
    return it == cache->end() ? nullptr : it->second;
}

bool ISerialization::isSpecialCompressionAllowed(const SubstreamPath & path)
{
    for (const auto & elem : path)
    {
        if (elem.type == Substream::NullMap
            || elem.type == Substream::ArraySizes
            || elem.type == Substream::DictionaryIndexes
            || elem.type == Substream::SparseOffsets)
            return false;
    }
    return true;
}

void ISerialization::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    /// Read until \t or \n.
    readString(field, istr);
    ReadBufferFromString buf(field);
    deserializeWholeText(column, buf, settings);
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

size_t ISerialization::getArrayLevel(const SubstreamPath & path)
{
    size_t level = 0;
    for (const auto & elem : path)
        level += elem.type == Substream::ArrayElements;
    return level;
}

bool ISerialization::hasSubcolumnForPath(const SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    size_t last_elem = prefix_len - 1;
    return path[last_elem].type == Substream::NullMap
            || path[last_elem].type == Substream::TupleElement
            || path[last_elem].type == Substream::ArraySizes;
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
            res.type = res.type ? creator->create(res.type) : res.type;
            res.serialization = res.serialization ? creator->create(res.serialization) : res.serialization;
            res.column = res.column ? creator->create(res.column) : res.column;
        }
    }

    return res;
}

void ISerialization::throwUnexpectedDataAfterParsedValue(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const String & type_name) const
{
    WriteBufferFromOwnString ostr;
    serializeText(column, column.size() - 1, ostr, settings);
    throw Exception(
        ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
        "Unexpected data '{}' after parsed {} value '{}'",
        std::string(istr.position(), std::min(size_t(10), istr.available())),
        type_name,
        ostr.str());
}

}

