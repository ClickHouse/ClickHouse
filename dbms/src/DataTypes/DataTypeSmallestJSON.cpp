#include <DataTypes/DataTypeSmallestJSON.h>

#include <Common/CpuId.h>
#include <Common/config.h>
#include <common/StringRef.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustomSimpleTextSerialization.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnSmallestJSON.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/RapidJSONParser.h>
#include <Functions/SimdJSONParser.h>
#include <Common/escapeForFileName.h>
#include <rapidjson/writer.h>
#include <DataTypes/SmallestJSON/BufferSmallestJSONStream.h>
#include <DataTypes/SmallestJSON/SmallestJSONStreamFactory.h>
#include <DataTypes/SmallestJSON/SmallestJSONSerialization.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_JSON;
}

namespace
{
struct SerializeBinaryBulkStateSmallestJSON : public IDataType::SerializeBinaryBulkState
{
    std::map<String, IDataType::SerializeBinaryBulkStatePtr> states;

    IDataType::SerializeBinaryBulkStatePtr getOrCreateDataState(WriteBuffer * ostr, const String & state_name, const DataTypePtr & type)
    {
        auto state_iterator = states.find(state_name);

        if (state_iterator != states.end())
            return state_iterator->second;

        IDataType::SerializeBinaryBulkSettings settings;
        settings.getter = [&](const IDataType::SubstreamPath &) {return ostr; };

        IDataType::SerializeBinaryBulkStatePtr state = states[state_name];
        type->serializeBinaryBulkStatePrefix(settings, state);
        return state;

    }

    IDataType::SerializeBinaryBulkStatePtr getOrCreateDataState(WriteBuffer * ostr, const StringRefs & /*data_column_name*/, const DataTypePtr & data_column_type)
    {
        const String & type_name = data_column_type->getName();

        writeBinary(type_name, *ostr);
        if (!data_column_type->haveSubtypes())
            return IDataType::SerializeBinaryBulkStatePtr{};

        return getOrCreateDataState(ostr, type_name, data_column_type);
    }
};

struct DeserializeBinaryBulkStateSmallestJSON : public IDataType::DeserializeBinaryBulkState
{
    IDataType::DeserializeBinaryBulkStatePtr null;
    std::vector<IDataType::DeserializeBinaryBulkStatePtr> states;

    std::tuple<DataTypePtr, IDataType::DeserializeBinaryBulkStatePtr *> getOrCreateDataState(ReadBuffer * istr, const StringRefs & /*data_column_name*/)
    {
        String type_name;
        readBinary(type_name, *istr);

        const DataTypePtr data_column_type = DataTypeFactory::instance().get(type_name);

        if (!data_column_type->haveSubtypes())
            return std::make_pair(data_column_type, &null);

        throw Exception("Exception", ErrorCodes::LOGICAL_ERROR);
    }
};

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

static SerializeBinaryBulkStateSmallestJSON * checkAndGetSmallestJSONSerializeState(IDataType::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeSmallestJSON.", ErrorCodes::LOGICAL_ERROR);

    auto * smallest_json_state = typeid_cast<SerializeBinaryBulkStateSmallestJSON *>(state.get());
    if (!smallest_json_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid SerializeBinaryBulkState for DataTypeSmallestJSON. Expected: "
                        + demangle(typeid(SerializeBinaryBulkStateSmallestJSON).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return smallest_json_state;
}

static DeserializeBinaryBulkStateSmallestJSON * checkAndGetSmallestJSONDeserializeState(IDataType::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeSmallestJSON.", ErrorCodes::LOGICAL_ERROR);

    auto * smallest_json_state = typeid_cast<DeserializeBinaryBulkStateSmallestJSON *>(state.get());
    if (!smallest_json_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid DeserializeBinaryBulkState for DataTypeSmallestJSON. Expected: "
                        + demangle(typeid(DeserializeBinaryBulkStateSmallestJSON).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return smallest_json_state;
}

}

inline static String sub_type_name(const String & prefix, const DataTypePtr & type)
{
    String type_index = toString(size_t(type->getTypeId()));
    return prefix.empty() ? type_index : prefix + "." + type_index;
}

DataTypeSmallestJSON::DataTypeSmallestJSON(const DataTypes & nested_types)
    : support_types(nested_types)
{
}

MutableColumnPtr DataTypeSmallestJSON::createColumn() const
{
    return ColumnSmallestJSON::create();
}

void DataTypeSmallestJSON::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    DataTypeString().serializeBinary(field, ostr);
}

void DataTypeSmallestJSON::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    DataTypeString().deserializeBinary(field, istr);
}

void DataTypeSmallestJSON::deserializeWholeText(IColumn & /*column*/, ReadBuffer & /*istr*/, const FormatSettings & /*settings*/) const
{
    throw Exception("", ErrorCodes::LOGICAL_ERROR);
}

void DataTypeSmallestJSON::serializeProtobuf(const IColumn &, size_t, ProtobufWriter &, size_t &) const
{
    throw Exception("Method serializeProtobuf is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void DataTypeSmallestJSON::deserializeProtobuf(IColumn &, ProtobufReader &, bool, bool &) const
{
    throw Exception("Method deserializeProtobuf is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void DataTypeSmallestJSON::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::deserialize(column, settings, SmallestJSONStreamFactory::fromBuffer<FormatStyle::CSV>(&istr, settings));
}

void DataTypeSmallestJSON::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::deserialize(column, settings, SmallestJSONStreamFactory::fromBuffer<FormatStyle::JSON>(&istr, settings));
}

void DataTypeSmallestJSON::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::deserialize(column, settings, SmallestJSONStreamFactory::fromBuffer<FormatStyle::QUOTED>(&istr, settings));
}

void DataTypeSmallestJSON::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::deserialize(column, settings, SmallestJSONStreamFactory::fromBuffer<FormatStyle::ESCAPED>(&istr, settings));
}

void DataTypeSmallestJSON::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::serialize(column, row_num, SmallestJSONStreamFactory::fromBuffer<FormatStyle::ESCAPED>(&ostr, settings));
}

void DataTypeSmallestJSON::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::serialize(column, row_num, SmallestJSONStreamFactory::fromBuffer<FormatStyle::JSON>(&ostr, settings));
}

void DataTypeSmallestJSON::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::serialize(column, row_num, SmallestJSONStreamFactory::fromBuffer<FormatStyle::CSV>(&ostr, settings));
}

void DataTypeSmallestJSON::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::serialize(column, row_num, SmallestJSONStreamFactory::fromBuffer<FormatStyle::QUOTED>(&ostr, settings));
}

void DataTypeSmallestJSON::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::serialize(column, row_num, SmallestJSONStreamFactory::fromBuffer<FormatStyle::ESCAPED>(&ostr, settings));
}

void DataTypeSmallestJSON::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    SmallestJSONSerialization::serialize(column, row_num, SmallestJSONStreamFactory::fromBuffer<FormatStyle::ESCAPED>(&ostr, settings));
}

void DataTypeSmallestJSON::serializeBinaryBulkStatePrefix(SerializeBinaryBulkSettings & /*settings*/, SerializeBinaryBulkStatePtr & state) const
{
    /// TODO: version
    state = std::make_shared<SerializeBinaryBulkStateSmallestJSON>();
}

void DataTypeSmallestJSON::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
//    auto * smallest_json_state = checkAndGetSmallestJSONSerializeState(state);
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

void DataTypeSmallestJSON::deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & /*settings*/, DeserializeBinaryBulkStatePtr & state) const
{
    /// TODO: version
    state = std::make_shared<DeserializeBinaryBulkStateSmallestJSON>();
}

void DataTypeSmallestJSON::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state_) const
{
    auto * serialize_state = checkAndGetSmallestJSONSerializeState(state_);
    const auto & smallest_json_column = static_cast<const ColumnSmallestJSON &>(column);

    settings.path.push_back(Substream::JSONInfo);
    if (WriteBuffer * info_stream = settings.getter(settings.path))
    {
        writeVarUInt(column.size(), *info_stream);
        auto smallest_json_struct = smallest_json_column.getStruct();
        smallest_json_struct->serialize(*info_stream, settings, [&](const JSONStructAndDataColumn & json_struct, SerializeBinaryBulkSettings & data_settings)
        {
            data_settings.path.back().type = Substream::JSONMask;
            DataTypeUInt8().serializeBinaryBulk(*json_struct.mark_column, *data_settings.getter(data_settings.path), offset, limit);

            String sub_name_prefix = data_settings.path.back().tuple_element_name;

            for (const auto & type_and_column : json_struct.data_columns)
            {
                data_settings.path.back().type = Substream::JSONElements;
                data_settings.path.back().tuple_element_name = sub_type_name(sub_name_prefix, type_and_column.first);
                auto data_serialize_state = serialize_state->getOrCreateDataState(info_stream, json_struct.access_path, type_and_column.first);
                type_and_column.first->serializeBinaryBulkWithMultipleStreams(*type_and_column.second, offset, limit, data_settings, data_serialize_state);
            }
        });
    }

    settings.path.pop_back();
}

void DataTypeSmallestJSON::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column_, size_t limit, DeserializeBinaryBulkSettings & settings_, DeserializeBinaryBulkStatePtr & state_) const
{
    CachedDeserializeBinaryBulkSettings settings(settings_);
    auto * serialize_state = checkAndGetSmallestJSONDeserializeState(state_);

    auto readPartOfJSONColumn = [&](IColumn & column, ReadBuffer * info_stream, size_t rows)
    {
        ColumnSmallestJSON & smallest_json_column = typeid_cast<ColumnSmallestJSON &>(column);

        auto smallest_json_struct = smallest_json_column.getStruct();
        smallest_json_struct->deserialize(*info_stream, settings,
            [&](JSONStructAndDataColumn & json_struct, size_t data_size, DeserializeBinaryBulkSettings & data_settings)
        {
            data_settings.path.back().type = Substream::JSONMask;
            DataTypeUInt8().deserializeBinaryBulk(*json_struct.getOrCreateMarkColumn(), *data_settings.getter(data_settings.path), rows, 0);

            String sub_name_prefix = data_settings.path.back().tuple_element_name;

            for (size_t index = 0; index < data_size; ++index)
            {
                const auto & [type, data_state] = serialize_state->getOrCreateDataState(info_stream, json_struct.access_path);

                data_settings.avg_value_size_hint = 0;
                data_settings.path.back().type = Substream::JSONElements;
                data_settings.path.back().tuple_element_name = sub_type_name(sub_name_prefix, type);
                type->deserializeBinaryBulkWithMultipleStreams(*json_struct.getOrCreateDataColumn(type), rows, data_settings, *data_state);
            }
        });
    };

    settings.path.push_back(Substream::JSONInfo);
    if (ReadBuffer * info_stream = settings.getter(settings.path))
    {
        ColumnSmallestJSON & smallest_json_column = typeid_cast<ColumnSmallestJSON &>(column_);

        while (limit)
        {
            if (info_stream->eof())
                break;

            size_t rows_to_read;
            readVarUInt(rows_to_read, *info_stream);
            size_t num_rows_to_read = std::min<UInt64>(limit, rows_to_read);

            MutableColumnPtr temp_column = ColumnSmallestJSON::create();
            readPartOfJSONColumn(*temp_column.get(), info_stream, num_rows_to_read);

            if (temp_column->size() != num_rows_to_read)
                throw Exception("LOGICAL_ERROR: column size: " + toString(temp_column->size()) + ", num_rows_to_read:" + toString(num_rows_to_read),
                    ErrorCodes::LOGICAL_ERROR);

            smallest_json_column.insertRangeFrom(*temp_column, 0, num_rows_to_read);
            limit -= num_rows_to_read;
        }
    }

    settings.path.pop_back();
}

void DataTypeSmallestJSON::enumerateStreams(const IDataType::StreamCallback & callback, IDataType::SubstreamPath & path) const
{
    path.push_back(Substream::JSONInfo);
    callback(path);
    path.back() = Substream::JSONElements;
    callback(path);
    path.pop_back();
}

void registerDataTypeSmallestJSON(DataTypeFactory & factory)
{
    /// bool, number, string
    static const DataTypes sub_types = {
        std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>()};

    factory.registerSimpleDataType("SmallestJSON", [&]() { return std::make_shared<DataTypeSmallestJSON>(sub_types); });
}

}


