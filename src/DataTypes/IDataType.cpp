#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/SipHash.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/NestedUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
    extern const int ILLEGAL_COLUMN;
}

String IDataType::Substream::toString() const
{
    switch (type)
    {
        case ArrayElements:
            return "ArrayElements";
        case ArraySizes:
            return "ArraySizes";
        case NullableElements:
            return "NullableElements";
        case NullMap:
            return "NullMap";
        case TupleElement:
            return "TupleElement(" + tuple_element_name + ", "
                + std::to_string(escape_tuple_delimiter) + ")";
        case DictionaryKeys:
            return "DictionaryKeys";
        case DictionaryIndexes:
            return "DictionaryIndexes";
    }

    __builtin_unreachable();
}

String IDataType::SubstreamPath::toString() const
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

IDataType::IDataType() : custom_name(nullptr), custom_text_serialization(nullptr), custom_streams(nullptr)
{
}

IDataType::~IDataType() = default;

String IDataType::getName() const
{
    if (custom_name)
    {
        return custom_name->getName();
    }
    else
    {
        return doGetName();
    }
}

String IDataType::doGetName() const
{
    return getFamilyName();
}

void IDataType::updateAvgValueSizeHint(const IColumn & column, double & avg_value_size_hint)
{
    /// Update the average value size hint if amount of read rows isn't too small
    size_t column_size = column.size();
    if (column_size > 10)
    {
        double current_avg_value_size = static_cast<double>(column.byteSize()) / column_size;

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint)
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        else if (current_avg_value_size * 2 < avg_value_size_hint)
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
    }
}

ColumnPtr IDataType::createColumnConst(size_t size, const Field & field) const
{
    auto column = createColumn();
    column->insert(field);
    return ColumnConst::create(std::move(column), size);
}


ColumnPtr IDataType::createColumnConstWithDefaultValue(size_t size) const
{
    return createColumnConst(size, getDefault());
}

DataTypePtr IDataType::promoteNumericType() const
{
    throw Exception("Data type " + getName() + " can't be promoted.", ErrorCodes::DATA_TYPE_CANNOT_BE_PROMOTED);
}

void IDataType::serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const
{
    throw Exception("Data type " + getName() + " must be serialized with multiple streams", ErrorCodes::MULTIPLE_STREAMS_REQUIRED);
}

void IDataType::deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, double) const
{
    throw Exception("Data type " + getName() + " must be deserialized with multiple streams", ErrorCodes::MULTIPLE_STREAMS_REQUIRED);
}

size_t IDataType::getSizeOfValueInMemory() const
{
    throw Exception("Value of type " + getName() + " in memory is not of fixed size.", ErrorCodes::LOGICAL_ERROR);
}

DataTypePtr IDataType::getSubcolumnType(const String & subcolumn_name) const
{
    auto subcolumn_type = tryGetSubcolumnType(subcolumn_name);
    if (subcolumn_type)
        return subcolumn_type;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

ColumnPtr IDataType::getSubcolumn(const String & subcolumn_name, const IColumn &) const
{
    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

Names IDataType::getSubcolumnNames() const
{
    NameSet res;
    enumerateStreams([&res, this](const SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        SubstreamPath new_path;
        /// Iterate over path to try to get intermediate subcolumns for complex nested types.
        for (const auto & elem : substream_path)
        {
            new_path.push_back(elem);
            auto subcolumn_name = getSubcolumnNameForStream(new_path);
            if (!subcolumn_name.empty() && tryGetSubcolumnType(subcolumn_name))
                res.insert(subcolumn_name);
        }
    });

    return Names(std::make_move_iterator(res.begin()), std::make_move_iterator(res.end()));
}

static String getNameForSubstreamPath(
    String stream_name,
    const IDataType::SubstreamPath & path,
    bool escape_tuple_delimiter)
{
    size_t array_level = 0;
    for (const auto & elem : path)
    {
        if (elem.type == IDataType::Substream::NullMap)
            stream_name += ".null";
        else if (elem.type == IDataType::Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (elem.type == IDataType::Substream::ArrayElements)
            ++array_level;
        else if (elem.type == IDataType::Substream::DictionaryKeys)
            stream_name += ".dict";
        else if (elem.type == IDataType::Substream::TupleElement)
        {
            /// For compatibility reasons, we use %2E (escaped dot) instead of dot.
            /// Because nested data may be represented not by Array of Tuple,
            ///  but by separate Array columns with names in a form of a.b,
            ///  and name is encoded as a whole.
            stream_name += (escape_tuple_delimiter && elem.escape_tuple_delimiter ?
                escapeForFileName(".") : ".") + escapeForFileName(elem.tuple_element_name);
        }
    }

    return stream_name;
}

String IDataType::getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path)
{
    auto name_in_storage = column.getNameInStorage();
    auto nested_storage_name = Nested::extractTableName(name_in_storage);

    if (name_in_storage != nested_storage_name && (path.size() == 1 && path[0].type == IDataType::Substream::ArraySizes))
        name_in_storage = nested_storage_name;

    auto stream_name = escapeForFileName(name_in_storage);
    return getNameForSubstreamPath(std::move(stream_name), path, true);
}

String IDataType::getSubcolumnNameForStream(const SubstreamPath & path)
{
    auto subcolumn_name = getNameForSubstreamPath("", path, false);
    if (!subcolumn_name.empty())
        subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.

    return subcolumn_name;
}

bool IDataType::isSpecialCompressionAllowed(const SubstreamPath & path)
{
    for (const Substream & elem : path)
    {
        if (elem.type == Substream::NullMap
            || elem.type == Substream::ArraySizes
            || elem.type == Substream::DictionaryIndexes)
            return false;
    }
    return true;
}

void IDataType::insertDefaultInto(IColumn & column) const
{
    column.insertDefault();
}

void IDataType::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    if (custom_streams)
        custom_streams->enumerateStreams(callback, path);
    else
        enumerateStreamsImpl(callback, path);
}

void IDataType::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (custom_streams)
        custom_streams->serializeBinaryBulkStatePrefix(settings, state);
    else
        serializeBinaryBulkStatePrefixImpl(settings, state);
}

void IDataType::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (custom_streams)
        custom_streams->serializeBinaryBulkStateSuffix(settings, state);
    else
        serializeBinaryBulkStateSuffixImpl(settings, state);
}

void IDataType::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    if (custom_streams)
        custom_streams->deserializeBinaryBulkStatePrefix(settings, state);
    else
        deserializeBinaryBulkStatePrefixImpl(settings, state);
}

void IDataType::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (custom_streams)
        custom_streams->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
    else
        serializeBinaryBulkWithMultipleStreamsImpl(column, offset, limit, settings, state);
}

void IDataType::deserializeBinaryBulkWithMultipleStreamsImpl(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * /* cache */) const
{
    if (ReadBuffer * stream = settings.getter(settings.path))
        deserializeBinaryBulk(column, *stream, limit, settings.avg_value_size_hint);
}


void IDataType::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (custom_streams)
    {
        custom_streams->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
        return;
    }

    /// Do not cache complex type, because they can be constructed
    /// from their subcolumns, which are in cache.
    if (!haveSubtypes())
    {
        auto cached_column = getFromSubstreamsCache(cache, settings.path);
        if (cached_column)
        {
            column = cached_column;
            return;
        }
    }

    auto mutable_column = column->assumeMutable();
    deserializeBinaryBulkWithMultipleStreamsImpl(*mutable_column, limit, settings, state, cache);
    column = std::move(mutable_column);

    if (!haveSubtypes())
        addToSubstreamsCache(cache, settings.path, column);
}

void IDataType::serializeAsTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->serializeTextEscaped(column, row_num, ostr, settings);
    else
        serializeTextEscaped(column, row_num, ostr, settings);
}

void IDataType::deserializeAsTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->deserializeTextEscaped(column, istr, settings);
    else
        deserializeTextEscaped(column, istr, settings);
}

void IDataType::serializeAsTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->serializeTextQuoted(column, row_num, ostr, settings);
    else
        serializeTextQuoted(column, row_num, ostr, settings);
}

void IDataType::deserializeAsTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->deserializeTextQuoted(column, istr, settings);
    else
        deserializeTextQuoted(column, istr, settings);
}

void IDataType::serializeAsTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->serializeTextCSV(column, row_num, ostr, settings);
    else
        serializeTextCSV(column, row_num, ostr, settings);
}

void IDataType::deserializeAsTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->deserializeTextCSV(column, istr, settings);
    else
        deserializeTextCSV(column, istr, settings);
}

void IDataType::serializeAsText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->serializeText(column, row_num, ostr, settings);
    else
        serializeText(column, row_num, ostr, settings);
}

void IDataType::deserializeAsWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->deserializeWholeText(column, istr, settings);
    else
        deserializeWholeText(column, istr, settings);
}

void IDataType::serializeAsTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->serializeTextJSON(column, row_num, ostr, settings);
    else
        serializeTextJSON(column, row_num, ostr, settings);
}

void IDataType::deserializeAsTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->deserializeTextJSON(column, istr, settings);
    else
        deserializeTextJSON(column, istr, settings);
}

void IDataType::serializeAsTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (custom_text_serialization)
        custom_text_serialization->serializeTextXML(column, row_num, ostr, settings);
    else
        serializeTextXML(column, row_num, ostr, settings);
}

void IDataType::setCustomization(DataTypeCustomDescPtr custom_desc_) const
{
    /// replace only if not null
    if (custom_desc_->name)
        custom_name = std::move(custom_desc_->name);

    if (custom_desc_->text_serialization)
        custom_text_serialization = std::move(custom_desc_->text_serialization);

    if (custom_desc_->streams)
        custom_streams = std::move(custom_desc_->streams);
}

void IDataType::addToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column)
{
    if (cache && !path.empty())
        cache->emplace(getSubcolumnNameForStream(path), column);
}

ColumnPtr IDataType::getFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path)
{
    if (!cache || path.empty())
        return nullptr;

    auto it = cache->find(getSubcolumnNameForStream(path));
    if (it == cache->end())
        return nullptr;

    return it->second;
}

}
