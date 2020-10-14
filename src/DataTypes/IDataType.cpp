#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>

#include <IO/WriteHelpers.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/NestedUtils.h>
#include <boost/algorithm/string/split.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
    extern const int ILLEGAL_COLUMN;
}

IDataType::IDataType() : custom_name(nullptr), custom_text_serialization(nullptr)
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

MutableColumnPtr IDataType::getSubcolumn(const String & subcolumn_name, IColumn &) const
{
    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

std::vector<String> IDataType::getSubcolumnNames() const
{
    std::vector<String> res;
    enumerateStreams([&res, this](const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        auto subcolumn_name = IDataType::getSubcolumnNameForStream("", substream_path);
        if (!subcolumn_name.empty())
        {
            subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.
            /// Not all of substreams have its subcolumn.
            if (tryGetSubcolumnType(subcolumn_name))
                res.push_back(subcolumn_name);
        }
    });

    return res;
}

static String getNameForSubstreamPath(
    String stream_name,
    const IDataType::SubstreamPath & path,
    const String & tuple_element_delimeter = ".")
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
        else if (elem.type == IDataType::Substream::TupleElement)
            stream_name += tuple_element_delimeter + escapeForFileName(elem.tuple_element_name);
        else if (elem.type == IDataType::Substream::DictionaryKeys)
            stream_name += ".dict";
    }

    return stream_name;
}

static bool isOldStyleNestedSizes(const NameAndTypePair & column, const IDataType::SubstreamPath & path)
{
    auto storage_name = column.getStorageName();
    auto nested_storage_name = Nested::extractTableName(column.getStorageName());

    if (storage_name == nested_storage_name)
        return false;

    return (path.size() == 1 && path[0].type == IDataType::Substream::ArraySizes) || column.getSubcolumnName() == "size0";
}

static String getDelimiterForSubcolumnPart(const String & subcolumn_part)
{
    if (subcolumn_part == "null" || startsWith(subcolumn_part, "size"))
        return ".";

    return "%2E";
}

/// FIXME: rewrite it.
String IDataType::getFileNameForStream(const NameAndTypePair & column, const IDataType::SubstreamPath & path)
{
    auto storage_name = column.getStorageName();
    if (isOldStyleNestedSizes(column, path))
        storage_name = Nested::extractTableName(storage_name);

    auto stream_name = escapeForFileName(storage_name);
    auto subcolumn_name = column.getSubcolumnName();

    if (!subcolumn_name.empty())
    {
        std::vector<String> subcolumn_parts;
        boost::split(subcolumn_parts, subcolumn_name, [](char c) { return c == '.'; });

        size_t current_nested_level = 0;
        for (const auto & elem : path)
        {
            if (elem.type == Substream::ArrayElements && elem.is_part_of_nested)
            {
                ++current_nested_level;
            }
            else if (elem.type == Substream::ArraySizes)
            {
                size_t nested_level = column.type->getNestedLevel();

                for (size_t i = 0; i < nested_level - current_nested_level; ++i)
                {
                    if (subcolumn_parts.empty())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get substream name for column {}."
                            " Not enough subcolumn parts. Needed: {}", column.name, nested_level - current_nested_level);

                    subcolumn_parts.pop_back();
                }
            }
        }

        for (const auto & subcolumn_part : subcolumn_parts)
            stream_name += getDelimiterForSubcolumnPart(subcolumn_part) + escapeForFileName(subcolumn_part);
    }

    return getNameForSubstreamPath(std::move(stream_name), path, "%2E");
}

String IDataType::getFileNameForStream(const String & column_name, const IDataType::SubstreamPath & path)
{
    /// Sizes of arrays (elements of Nested type) are shared (all reside in single file).
    String nested_table_name = Nested::extractTableName(column_name);

    bool is_sizes_of_nested_type =
        path.size() == 1    /// Nested structure may have arrays as nested elements (so effectively we have multidimensional arrays).
                            /// Sizes of arrays are shared only at first level.
        && path[0].type == IDataType::Substream::ArraySizes
        && nested_table_name != column_name;

    auto stream_name = escapeForFileName(is_sizes_of_nested_type ? nested_table_name : column_name);

    /// For compatibility reasons, we use %2E instead of dot.
    /// Because nested data may be represented not by Array of Tuple,
    ///  but by separate Array columns with names in a form of a.b,
    ///  and name is encoded as a whole.
    return getNameForSubstreamPath(std::move(stream_name), path, "%2E");
}

String IDataType::getSubcolumnNameForStream(String stream_name, const SubstreamPath & path)
{
    return getNameForSubstreamPath(std::move(stream_name), path);
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
}

}
