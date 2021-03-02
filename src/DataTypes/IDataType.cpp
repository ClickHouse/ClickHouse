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
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationTupleElement.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
    extern const int ILLEGAL_COLUMN;
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
    getDefaultSerialization()->enumerateStreams([&res, this](const ISerialization::SubstreamPath & substream_path)
    {
        ISerialization::SubstreamPath new_path;
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
    const ISerialization::SubstreamPath & path,
    bool escape_tuple_delimiter)
{
    size_t array_level = 0;
    for (const auto & elem : path)
    {
        if (elem.type == ISerialization::Substream::NullMap)
            stream_name += ".null";
        else if (elem.type == ISerialization::Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (elem.type == ISerialization::Substream::ArrayElements)
            ++array_level;
        else if (elem.type == ISerialization::Substream::DictionaryKeys)
            stream_name += ".dict";
        else if (elem.type == ISerialization::Substream::TupleElement)
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

String IDataType::getFileNameForStream(const NameAndTypePair & column, const ISerialization::SubstreamPath & path)
{
    auto name_in_storage = column.getNameInStorage();
    auto nested_storage_name = Nested::extractTableName(name_in_storage);

    if (name_in_storage != nested_storage_name && (path.size() == 1 && path[0].type == ISerialization::Substream::ArraySizes))
        name_in_storage = nested_storage_name;

    auto stream_name = escapeForFileName(name_in_storage);
    return getNameForSubstreamPath(std::move(stream_name), path, true);
}

String IDataType::getSubcolumnNameForStream(const ISerialization::SubstreamPath & path)
{
    auto subcolumn_name = getNameForSubstreamPath("", path, false);
    if (!subcolumn_name.empty())
        subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.

    return subcolumn_name;
}

bool IDataType::isSpecialCompressionAllowed(const ISerialization::SubstreamPath & path)
{
    for (const ISerialization::Substream & elem : path)
    {
        if (elem.type == ISerialization::Substream::NullMap
            || elem.type == ISerialization::Substream::ArraySizes
            || elem.type == ISerialization::Substream::DictionaryIndexes)
            return false;
    }
    return true;
}

void IDataType::insertDefaultInto(IColumn & column) const
{
    column.insertDefault();
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
    
    if (custom_desc_->serialization)
        custom_serialization = std::move(custom_desc_->serialization);
}

SerializationPtr IDataType::getDefaultSerialization() const
{
    if (custom_serialization)
        return custom_serialization;
    
    return doGetDefaultSerialization();
}

SerializationPtr IDataType::doGetDefaultSerialization() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no serialization in type {}", getName());
}

SerializationPtr IDataType::getSparseSerialization() const
{
    return std::make_shared<SerializationSparse>(getDefaultSerialization());
}

SerializationPtr IDataType::getSubcolumnSerialization(const String & subcolumn_name, const SerializationPtr &) const
{
    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

SerializationPtr IDataType::getSerialization(const String & column_name, const SerializationInfo & info) const
{
    ISerialization::Settings settings = 
    {
        .num_rows = info.getNumberOfRows(),
        .num_non_default_rows = info.getNumberOfNonDefaultValues(column_name),
        .min_ratio_for_dense_serialization = 10
    };

    return getSerialization(settings);
}

SerializationPtr IDataType::getSerialization(const IColumn & column) const
{
    ISerialization::Settings settings =
    {
        .num_rows = column.size(),
        .num_non_default_rows = column.getNumberOfNonDefaultValues(),
        .min_ratio_for_dense_serialization = 10
    };
    
    return getSerialization(settings);
}

SerializationPtr IDataType::getSerialization(const ISerialization::Settings & settings) const
{
    // if (settings.num_non_default_rows * settings.min_ratio_for_dense_serialization < settings.num_rows)
    //     return getSparseSerialization();

    UNUSED(settings);
    
    return getDefaultSerialization();
}

// static
SerializationPtr IDataType::getSerialization(const NameAndTypePair & column, const IDataType::StreamExistenceCallback & callback)
{
    auto base_serialization = column.type->getSerialization(column.name, callback);
    if (column.isSubcolumn())
        return column.getTypeInStorage()->getSubcolumnSerialization(column.getSubcolumnName(), base_serialization);
    
    return base_serialization;
}

SerializationPtr IDataType::getSerialization(const String & column_name, const StreamExistenceCallback & callback) const
{
    // auto sparse_idx_name = escapeForFileName(column_name) + ".sparse.idx";
    // if (callback(sparse_idx_name))
    //     return getSparseSerialization();

    UNUSED(column_name);
    UNUSED(callback);
    
    return getDefaultSerialization();
}

DataTypePtr IDataType::getTypeForSubstream(const ISerialization::SubstreamPath & substream_path) const
{
    auto type = tryGetSubcolumnType(ISerialization::getSubcolumnNameForStream(substream_path));
    if (type)
        return type;
    
    return shared_from_this();
}

void IDataType::enumerateStreams(const SerializationPtr & serialization, const SubstreamCallback & callback, ISerialization::SubstreamPath & path) const
{
    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        callback(substream_path, *getTypeForSubstream(substream_path));
    }, path);
}

}
