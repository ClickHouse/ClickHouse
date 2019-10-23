#include <DataTypes/DataTypeJSONB.h>

#include <Common/CpuId.h>
#include <Common/config.h>
#include <common/StringRef.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnJSONB.h>
#include <Columns/ColumnString.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/JSONBStreamBuffer.h>
#include <DataTypes/JSONBStreamFactory.h>
#include <DataTypes/JSONBSerialization.h>
#include <DataTypes/JSONBSerializeBinaryBulkState.h>
#include <DataTypes/JSONBDeserializeBinaryBulkState.h>
#include "DataTypeJSONB.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_JSON;
}


namespace
{

struct CachedDeserializeBinaryBulkSettings : public IDataType::DeserializeBinaryBulkSettings
{
    IDataType::InputStreamGetter origin_getter;
    std::map<String, ReadBuffer *> read_buffer_cache;

    CachedDeserializeBinaryBulkSettings(IDataType::DeserializeBinaryBulkSettings & settings_)
    {
        path = settings_.path;
        origin_getter = settings_.getter;
        continuous_reading = settings_.continuous_reading;
        avg_value_size_hint = settings_.avg_value_size_hint;
        position_independent_encoding = settings_.position_independent_encoding;
        getter = [&] (const IDataType::SubstreamPath & substream) { return cached_get(substream); };
    }


    ReadBuffer * cached_get(const IDataType::SubstreamPath & substream)
    {
        const String & stream_name = IDataType::getFileNameForStream("dummy", substream);

        auto buffer_iterator = read_buffer_cache.find(stream_name);

        if (buffer_iterator != read_buffer_cache.end())
            return buffer_iterator->second;

        read_buffer_cache[stream_name] = origin_getter(substream);
        return read_buffer_cache[stream_name];
    }
};

    size_t fixAndGetLimit(size_t offset, size_t limit, size_t column_size)
    {
        if (limit == 0 || offset + limit > column_size)
            return column_size - offset;
        return limit;
    }
}

MutableColumnPtr DataTypeJSONB::createColumn() const
{
    /// TODO: Array(size_t) -> dynamic selection in Array(UInt8), Array(UInt16), Array(UInt32), Array(UInt64)
    /// Relational data is represented by Unique(Array(size_t)) types, but stored by Unique(String) types in storage
    Columns binary_data_columns(2);
    binary_data_columns[0] = ColumnArray::create(ColumnUInt64::create());
    binary_data_columns[1] = ColumnString::create();
    return ColumnJSONB::create(ColumnUnique<ColumnString>::create(DataTypeString()),
        ColumnUnique<ColumnArray>::create(DataTypeArray(std::make_shared<DataTypeUInt64>())), std::move(binary_data_columns),
        false, isNullable(), isLowCardinality());
}

void DataTypeJSONB::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    DataTypeString().serializeBinary(field, ostr);
}

void DataTypeJSONB::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    DataTypeString().deserializeBinary(field, istr);
}

void DataTypeJSONB::deserializeWholeText(IColumn & /*column*/, ReadBuffer & /*istr*/, const FormatSettings & /*settings*/) const
{
    throw Exception("Method deserializeWholeText is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void DataTypeJSONB::serializeProtobuf(const IColumn &, size_t, ProtobufWriter &, size_t &) const
{
    throw Exception("Method serializeProtobuf is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void DataTypeJSONB::deserializeProtobuf(IColumn &, ProtobufReader &, bool, bool &) const
{
    throw Exception("Method deserializeProtobuf is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void DataTypeJSONB::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    JSONBSerialization::deserialize(isNullable(), column, JSONBStreamFactory::from<FormatStyle::CSV>(&istr, settings));
}

void DataTypeJSONB::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    JSONBSerialization::deserialize(isNullable(), column, JSONBStreamFactory::from<FormatStyle::JSON>(&istr, settings));
}

void DataTypeJSONB::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    JSONBSerialization::deserialize(isNullable(), column, JSONBStreamFactory::from<FormatStyle::QUOTED>(&istr, settings));
}

void DataTypeJSONB::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    JSONBSerialization::deserialize(isNullable(), column, JSONBStreamFactory::from<FormatStyle::ESCAPED>(&istr, settings));
}

void DataTypeJSONB::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    JSONBSerialization::serialize(column, row_num, JSONBStreamFactory::from<FormatStyle::ESCAPED>(&ostr, settings));
}

void DataTypeJSONB::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    JSONBSerialization::serialize(column, row_num, JSONBStreamFactory::from<FormatStyle::JSON>(&ostr, settings));
}

void DataTypeJSONB::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    JSONBSerialization::serialize(column, row_num, JSONBStreamFactory::from<FormatStyle::CSV>(&ostr, settings));
}

void DataTypeJSONB::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    JSONBSerialization::serialize(column, row_num, JSONBStreamFactory::from<FormatStyle::QUOTED>(&ostr, settings));
}

void DataTypeJSONB::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    JSONBSerialization::serialize(column, row_num, JSONBStreamFactory::from<FormatStyle::ESCAPED>(&ostr, settings));
}

void DataTypeJSONB::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    JSONBSerialization::serialize(column, row_num, JSONBStreamFactory::from<FormatStyle::ESCAPED>(&ostr, settings));
}

void DataTypeJSONB::serializeBinaryBulkStatePrefix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & serialize_state) const
{
    serialize_state = std::make_shared<JSONBSerializeBinaryBulkState>(settings, 1);
}

void DataTypeJSONB::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
//    auto * smallest_json_state = checkAndGetJSONBSerializeState(state);
//    serializeBinaryBulkState(settings_, [&] (IDataType::SerializeBinaryBulkSettings & settings)
//    {
//        smallest_json_state->states.resize(support_types.size());
//        writeBinary(smallest_json_state->states.size(), *settings.getter(settings.path));
//        for (size_t i = 0; i < support_types.size(); ++i)
//            support_types[i]->serializeBinaryBulkStateSuffix(settings, smallest_json_state->states[i]);
//    });

//    std::cout << "rows: " << smallest_json_column.size() << "\n";
//    std::cout << "marks: " << smallest_json_column.getMarks().size() << "\n";
//    std::cout << "fields: " << smallest_json_column.getFields().size() << "\n";
}

void DataTypeJSONB::deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & deserialize_state) const
{
    deserialize_state = std::make_shared<JSONBDeserializeBinaryBulkState>(settings);
}

void DataTypeJSONB::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (size_t fixed_limit = fixAndGetLimit(offset, limit, column.size()))
    {
        const auto & serialize_state = JSONBSerializeBinaryBulkState::check(state);
        const auto & serialize_column = checkAndGetColumn<ColumnJSONB>(column)->convertToMultipleIfNeed(offset, fixed_limit);

        SCOPE_EXIT({settings.path.pop_back();});
        settings.path.push_back(Substream::JSONBinaryRelations);
        serialize_state->serializeRowRelationsColumn(*serialize_column, *settings.getter(settings.path), offset, fixed_limit);

        settings.path.back().type = Substream::JSONDictionaryKeys;
        serialize_state->serializeJSONDictionaryColumn(serialize_column->getKeysDictionary(), *settings.getter(settings.path));
        settings.path.back().type = Substream::JSONDictionaryRelations;
        serialize_state->serializeJSONDictionaryColumn(serialize_column->getRelationsDictionary(), *settings.getter(settings.path));

        settings.path.back().type = Substream::JSONBinaryMultipleData;
        serialize_state->serializeJSONBinaryMultipleColumn(*serialize_column, settings, offset, fixed_limit);
    }
}

void DataTypeJSONB::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column, size_t limit, DeserializeBinaryBulkSettings & settings_, DeserializeBinaryBulkStatePtr & state) const
{
    CachedDeserializeBinaryBulkSettings settings(settings_);

    SCOPE_EXIT({settings.path.pop_back();});
    settings.path.push_back(Substream::JSONBinaryRelations);
    if (ReadBuffer * relations_stream = settings.getter(settings.path))
    {
        auto & full_binary_column = static_cast<ColumnJSONB &>(column);
        const auto & deserialize_state = JSONBDeserializeBinaryBulkState::check(state);

        while (limit && !relations_stream->eof())
        {
            const auto & relations_info = deserialize_state->deserializeRowRelationsColumn(*relations_stream);

            settings.path.back().type = Substream::JSONDictionaryKeys;
            const auto & keys_dictionary = deserialize_state->deserializeJSONKeysDictionaryColumn(*settings.getter(settings.path));
            settings.path.back().type = Substream::JSONDictionaryRelations;
            const auto & relations_dictionary = deserialize_state->deserializeJSONRelationsDictionaryColumn(*settings.getter(settings.path));

            settings.path.back().type = Substream::JSONBinaryMultipleData;
            Columns data_columns = deserialize_state->deserializeJSONBinaryDataColumn(relations_info, settings);
            MutableColumnPtr current_read_column = ColumnJSONB::create(keys_dictionary, relations_dictionary, data_columns);

            size_t current_read_rows = std::min(limit, current_read_column->size());
            full_binary_column.insertRangeFrom(*current_read_column, 0, current_read_rows);
            limit -= current_read_rows;
        }
    }
}

DataTypeJSONB::DataTypeJSONB(bool is_nullable_, bool is_low_cardinality_)
    : is_nullable(is_nullable_), is_low_cardinality(is_low_cardinality_)
{
}

void DataTypeJSONB::enumerateStreams(const IDataType::StreamCallback & /*callback*/, IDataType::SubstreamPath & /*path*/) const
{
}

String DataTypeJSONB::doGetName() const
{
    if (isNullable())
        return "Nullable(JSONB)";

    return "JSONB";
}

void registerDataTypeJSONB(DataTypeFactory & factory)
{
    factory.registerDataType("JSONB", [&](const ASTPtr & /*arguments*/, std::vector<String> & full_types)
    {
        bool is_nullable = false, is_low_cardinality = false;

        /// Nullable(JSONB)
        is_nullable = full_types.size() >= 2 && full_types.back() == "JSONB" && Poco::toLower(full_types[full_types.size() - 2]) == "nullable";

        /// TODO: LowCardinality(JSONB), Nullable(LowCardinality(JSONB)) LowCardinality(Nullable(JSONB))
        return std::make_shared<DataTypeJSONB>(is_nullable, is_low_cardinality);
    });
}

}


