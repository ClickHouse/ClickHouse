#include <DataTypes/Serializations/SerializationMap.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationArrayOffsets.h>
#include <DataTypes/Serializations/SerializationMapKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMapSize.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Formats/JSONUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int INCORRECT_DATA;
}

UInt128 SerializationMap::getHash(const SerializationPtr & nested_, MergeTreeMapSerializationVersion serialization_version_)
{
    SipHash hash;
    hash.update("Map");
    hash.update(nested_->getHash());
    hash.update(static_cast<UInt8>(serialization_version_));
    return hash.get128();
}

SerializationMap::SerializationMap(
    const SerializationPtr & key_serialization_,
    const SerializationPtr & value_serialization_,
    const SerializationPtr & nested_serialization_,
    MergeTreeMapSerializationVersion serialization_version_)
    : key_serialization(key_serialization_)
    , value_serialization(value_serialization_)
    , nested_serialization(nested_serialization_)
    , serialization_version(serialization_version_)
{
}

static const IColumn & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnMap &>(column).getNestedColumn();
}

static IColumn & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnMap &>(column).getNestedColumn();
}

void SerializationMap::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & map = field.safeGet<Map>();
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<Tuple>();
        assert(tuple.size() == 2);
        key_serialization->serializeBinary(tuple[0], ostr, settings);
        value_serialization->serializeBinary(tuple[1], ostr, settings);
    }
}

void SerializationMap::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    size_t size;
    readVarUInt(size, istr);
    if (settings.binary.max_binary_array_size && size > settings.binary.max_binary_array_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Too large map size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_array_size",
            size,
            settings.binary.max_binary_array_size);
    field = Map();
    Map & map = field.safeGet<Map>();
    map.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        Tuple tuple(2);
        key_serialization->deserializeBinary(tuple[0], istr, settings);
        value_serialization->deserializeBinary(tuple[1], istr, settings);
        map.push_back(std::move(tuple));
    }
}

void SerializationMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeBinary(extractNestedColumn(column), row_num, ostr, settings);
}

void SerializationMap::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeBinary(extractNestedColumn(column), istr, settings);
}

void SerializationMap::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested_serialization->serializeForHashCalculation(extractNestedColumn(column), row_num, ostr);
}

void SerializationMap::readMapSafe(DB::IColumn & column, std::function<void()> && read_func)
{
    size_t initial_size = column.size();
    try
    {
        read_func();
    }
    catch (...)
    {
        ColumnMap & column_map = assert_cast<ColumnMap &>(column);
        ColumnArray & column_array = column_map.getNestedColumn();
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        ColumnTuple & nested_columns = column_map.getNestedData();
        IColumn & keys_column = nested_columns.getColumn(0);
        IColumn & values_column = nested_columns.getColumn(1);

        if (offsets.size() > initial_size)
        {
            chassert(offsets.size() - initial_size == 1);
            offsets.pop_back();
        }

        if (keys_column.size() > offsets.back())
            keys_column.popBack(keys_column.size() - offsets.back());

        if (values_column.size() > offsets.back())
            values_column.popBack(values_column.size() - offsets.back());

        throw;
    }
}

SerializationPtr SerializationMap::create(
    const SerializationPtr & key_serialization_,
    const SerializationPtr & value_serialization_,
    const SerializationPtr & nested_serialization_,
    MergeTreeMapSerializationVersion serialization_version_)
{
    if (!nested_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationMap(key_serialization_, value_serialization_, nested_serialization_, serialization_version_));
    return ISerialization::pooled(getHash(nested_serialization_, serialization_version_), [&] { return new SerializationMap(key_serialization_, value_serialization_, nested_serialization_, serialization_version_); });
}

template <typename KeyWriter, typename ValueWriter>
void SerializationMap::serializeTextImpl(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    KeyWriter && key_writer,
    ValueWriter && value_writer) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);

    const auto & nested_array = column_map.getNestedColumn();
    const auto & nested_tuple = column_map.getNestedData();
    const auto & offsets = nested_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    writeChar('{', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);

        key_writer(ostr, key_serialization, nested_tuple.getColumn(0), i);
        writeChar(':', ostr);
        value_writer(ostr, value_serialization, nested_tuple.getColumn(1), i);
    }
    writeChar('}', ostr);
}

template <typename ReturnType, typename Reader>
ReturnType SerializationMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader) const
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto & column_map = assert_cast<ColumnMap &>(column);

    auto & nested_array = column_map.getNestedColumn();
    auto & nested_tuple = column_map.getNestedData();
    auto & offsets = nested_array.getOffsets();

    auto & key_column = nested_tuple.getColumn(0);
    auto & value_column = nested_tuple.getColumn(1);

    size_t size = 0;
    if constexpr (throw_exception)
        assertChar('{', istr);
    else if (!checkChar('{', istr))
        return ReturnType(false);

    auto on_error_no_throw = [&]()
    {
        if (size)
        {
            nested_tuple.getColumnPtr(0) = key_column.cut(0, offsets.back());
            nested_tuple.getColumnPtr(1) = value_column.cut(0, offsets.back());
        }

        return ReturnType(false);
    };

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                {
                    ++istr.position();
                }
                else
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_READ_MAP_FROM_TEXT, "Cannot read Map from text");
                    return on_error_no_throw();
                }
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            if constexpr (throw_exception)
                reader(istr, key_serialization, key_column);
            else if (!reader(istr, key_serialization, key_column))
                return on_error_no_throw();

            ++size;

            skipWhitespaceIfAny(istr);
            if constexpr (throw_exception)
                assertChar(':', istr);
            else if (!checkChar(':', istr))
                return on_error_no_throw();
            skipWhitespaceIfAny(istr);

            if constexpr (throw_exception)
                reader(istr, value_serialization, value_column);
            else if (!reader(istr, value_serialization, value_column))
                return on_error_no_throw();

            skipWhitespaceIfAny(istr);
        }

        if constexpr (throw_exception)
            assertChar('}', istr);
        else if (!checkChar('}', istr))
            return on_error_no_throw();
    }
    catch (...)
    {
        if (size)
        {
            nested_tuple.getColumnPtr(0) = key_column.cut(0, offsets.back());
            nested_tuple.getColumnPtr(1) = value_column.cut(0, offsets.back());
        }

        if constexpr (throw_exception)
            throw;
        return ReturnType(false);
    }

    offsets.push_back(offsets.back() + size);
    return ReturnType(true);
}

void SerializationMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto writer = [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
    {
        subcolumn_serialization->serializeTextQuoted(subcolumn, pos, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, writer, writer);
}

void SerializationMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    deserializeTextImpl(column, istr,
        [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(subcolumn, buf, settings, subcolumn_serialization);
            else
                subcolumn_serialization->deserializeTextQuoted(subcolumn, buf, settings);
        });

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Map");
}

bool SerializationMap::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    auto reader = [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
    {
        if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
            return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(subcolumn, buf, settings, subcolumn_serialization);
        return subcolumn_serialization->tryDeserializeTextQuoted(subcolumn, buf, settings);
    };

    auto ok = deserializeTextImpl<bool>(column, istr, reader);
    if (!ok)
        return false;

    if (whole && !istr.eof())
    {
        column.popBack(1);
        return false;
    }

    return true;
}

void SerializationMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.json.write_map_as_array_of_tuples)
    {
        nested_serialization->serializeTextJSON(extractNestedColumn(column), row_num, ostr, settings);
        return;
    }

    serializeTextImpl(column, row_num, ostr,
        [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            /// We need to double-quote all keys (including integers) to produce valid JSON.
            WriteBufferFromOwnString str_buf;
            subcolumn_serialization->serializeText(subcolumn, pos, str_buf, settings);
            writeJSONString(str_buf.str(), buf, settings);
        },
        [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_serialization->serializeTextJSON(subcolumn, pos, buf, settings);
        });
}

void SerializationMap::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    if (settings.json.write_map_as_array_of_tuples)
    {
        nested_serialization->serializeTextJSONPretty(extractNestedColumn(column), row_num, ostr, settings, indent);
        return;
    }

    const auto & column_map = assert_cast<const ColumnMap &>(column);

    const auto & nested_array = column_map.getNestedColumn();
    const auto & nested_tuple = column_map.getNestedData();
    const auto & offsets = nested_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    if (offset == next_offset)
    {
        writeCString("{}", ostr);
        return;
    }

    writeCString("{\n", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeCString(",\n", ostr);

        WriteBufferFromOwnString str_buf;
        key_serialization->serializeText(nested_tuple.getColumn(0), i, str_buf, settings);

        writeChar(settings.json.pretty_print_indent, (indent + 1) * settings.json.pretty_print_indent_multiplier, ostr);
        writeJSONString(str_buf.str(), ostr, settings);
        writeCString(": ", ostr);
        value_serialization->serializeTextJSONPretty(nested_tuple.getColumn(1), i, ostr, settings, indent + 1);
    }
    writeChar('\n', ostr);
    writeChar(settings.json.pretty_print_indent, indent * settings.json.pretty_print_indent_multiplier, ostr);
    writeChar('}', ostr);
}


template <typename ReturnType>
ReturnType SerializationMap::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto deserialize_nested = [&settings](IColumn & subcolumn, ReadBuffer & buf, const SerializationPtr & subcolumn_serialization) -> ReturnType
    {
        if constexpr (std::is_same_v<ReturnType, void>)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(subcolumn, buf, settings, subcolumn_serialization);
            else
                subcolumn_serialization->deserializeTextJSON(subcolumn, buf, settings);
        }
        else
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(subcolumn, buf, settings, subcolumn_serialization);
            return subcolumn_serialization->tryDeserializeTextJSON(subcolumn, buf, settings);
        }
    };

    if (settings.json.empty_as_default)
        return deserializeTextImpl<ReturnType>(column, istr,
            [&deserialize_nested](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn) -> ReturnType
            {
                return JSONUtils::deserializeEmpyStringAsDefaultOrNested<ReturnType>(subcolumn, buf,
                    [&deserialize_nested, &subcolumn_serialization](IColumn & subcolumn_, ReadBuffer & buf_) -> ReturnType
                    {
                        return deserialize_nested(subcolumn_, buf_, subcolumn_serialization);
                    });
            });
    return deserializeTextImpl<ReturnType>(
        column,
        istr,
        [&deserialize_nested](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn) -> ReturnType
        { return deserialize_nested(subcolumn, buf, subcolumn_serialization); });
}

void SerializationMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.json.read_map_as_array_of_tuples)
    {
        nested_serialization->deserializeTextJSON(extractNestedColumn(column), istr, settings);
        return;
    }
    deserializeTextJSONImpl<void>(column, istr, settings);
}

bool SerializationMap::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.json.read_map_as_array_of_tuples)
    {
        return nested_serialization->tryDeserializeTextJSON(extractNestedColumn(column), istr, settings);
    }
    return deserializeTextJSONImpl<bool>(column, istr, settings);
}

void SerializationMap::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & offsets = column_map.getNestedColumn().getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const auto & nested_data = column_map.getNestedData();

    writeCString("<map>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        writeCString("<key>", ostr);
        key_serialization->serializeTextXML(nested_data.getColumn(0), i, ostr, settings);
        writeCString("</key>", ostr);

        writeCString("<value>", ostr);
        value_serialization->serializeTextXML(nested_data.getColumn(1), i, ostr, settings);
        writeCString("</value>", ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</map>", ostr);
}

void SerializationMap::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}

void SerializationMap::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings, true);
}

bool SerializationMap::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    if (!tryReadCSV(s, istr, settings.csv))
        return false;
    ReadBufferFromString rb(s);
    return tryDeserializeText(column, rb, settings, true);
}

/// Serialization state for the `with_buckets` Map format.
struct SerializeBinaryBulkStateMapWithBuckets : public ISerialization::SerializeBinaryBulkState
{
    /// Number of buckets the Map column is split into.
    size_t buckets = 1;
    /// When true, statistics are accumulated during
    /// serialization and written in the suffix rather than the prefix.
    bool recalculate_statistics = false;
    /// Accumulated statistics. Only meaningful when `recalculate_statistics` is true.
    ColumnMap::Statistics statistics;
    /// Per-bucket nested serialization states.
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> bucket_nested_states;

    SerializeBinaryBulkStateMapWithBuckets() = default;
};

/// Unified deserialization state for `SerializationMap`, used for both BASIC and WITH_BUCKETS modes.
struct DeserializeBinaryBulkStateMap : public ISerialization::DeserializeBinaryBulkState
{
    /// Shared reading info state, cached at the current substream path.
    ISerialization::DeserializeBinaryBulkStatePtr reading_info_state;

    /// --- BASIC mode fields ---
    /// Nested deserialization state for the single Array(Tuple(K, V)) stream.
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;

    /// --- WITH_BUCKETS mode fields ---
    /// State read from the buckets info stream.
    ISerialization::DeserializeBinaryBulkStatePtr buckets_info_state;
    /// Per-bucket nested deserialization states.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_nested_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMap>(*this);
        new_state->reading_info_state = reading_info_state ? reading_info_state->clone() : nullptr;
        new_state->nested_state = nested_state ? nested_state->clone() : nullptr;
        new_state->buckets_info_state = buckets_info_state ? buckets_info_state->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_nested_states.size(); ++bucket)
            new_state->bucket_nested_states[bucket] = bucket_nested_states[bucket] ? bucket_nested_states[bucket]->clone() : nullptr;
        return new_state;
    }
};

namespace
{

/// Subcolumn creator that wraps a nested Array subcolumn creator and produces
/// `SerializationMapKeysOrValues` for the serialization layer. Used during
/// `enumerateStreams` to expose virtual sub-streams (e.g. `map.keys`, `map.values`)
/// that know about the bucketed Map serialization format.
struct KeysOrValuesSubcolumnCreator : public ISerialization::ISubcolumnCreator
{
    std::shared_ptr<ISerialization::ISubcolumnCreator> nested_creator;
    MergeTreeMapSerializationVersion serialization_version;

    KeysOrValuesSubcolumnCreator(const std::shared_ptr<ISerialization::ISubcolumnCreator> & nested_creator_, MergeTreeMapSerializationVersion serialization_version_)
        : nested_creator(nested_creator_), serialization_version(serialization_version_)
    {
    }

    DataTypePtr create(const DataTypePtr & prev) const override { return nested_creator->create(prev); }
    ColumnPtr create(const ColumnPtr & prev) const override { return nested_creator->create(prev); }
    SerializationPtr create(const SerializationPtr & prev, const DataTypePtr & type) const override
    {
        return SerializationMapKeysOrValues::create(nested_creator->create(prev, type), serialization_version);
    }
};

}

void SerializationMap::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const DataTypeMap * map_type = data.type ? assert_cast<const DataTypeMap *>(data.type.get()) : nullptr;
    const ColumnMap * map_column = data.column ? assert_cast<const ColumnMap *>(data.column.get()) : nullptr;
    const auto * map_deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateMap>(data.deserialize_state) : nullptr;

    auto next_data = SubstreamData(nested_serialization)
        .withType(map_type ? map_type->getNestedType() : nullptr)
        .withColumn(map_column ? map_column->getNestedColumnPtr() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(map_deserialize_state ? map_deserialize_state->nested_state : nullptr);

    /// BASIC format stores the Map as a plain Array(Tuple(K, V)) — no bucketing.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        nested_serialization->enumerateStreams(settings, callback, next_data);
        return;
    }

    /// The buckets info stream (bucket count + optional statistics).
    settings.path.push_back(Substream::MapBucketsInfo);
    callback(settings.path);
    settings.path.pop_back();

    /// Virtual sub-streams are not stored on disk as separate streams. They are
    /// synthesized to expose Map subcolumns (map.size, map.keys, map.values) in
    /// the stream enumeration so that higher-level code (e.g. `FunctionToSubcolumnsPass`)
    /// can discover and read them. Each virtual stream wraps the real bucketed data
    /// with a serialization that knows how to reconstruct the subcolumn from buckets.
    if (settings.enumerate_virtual_streams)
    {
        /// Virtual stream for map.size — exposes per-row map sizes as a UInt64 subcolumn.
        /// Wrapped in `SerializationMapSize` which reads array offsets from all buckets
        /// and sums them to produce the total map size per row.
        settings.path.push_back(Substream::ArraySizes);
        auto subcolumn_name = "size" + std::to_string(settings.array_level);
        auto array_size_serialization = SerializationNamed::create(SerializationArrayOffsets::create(), subcolumn_name, SubstreamType::NamedOffsets);
        auto map_size_serialization = SerializationMapSize::create(array_size_serialization, serialization_version);
        settings.path.back().data = SubstreamData(map_size_serialization)
            .withType(map_type ? std::make_shared<DataTypeUInt64>() : nullptr)
            .withColumn(map_column ? map_column->getNestedColumn().getOffsetsPtr() : nullptr);
        callback(settings.path);
        settings.path.pop_back();

        /// Virtual stream for map.keys / map.values — exposes the Tuple(K, V) data
        /// as a subcolumn. Uses `KeysOrValuesSubcolumnCreator` to produce
        /// `SerializationMapKeysOrValues` that reads key/value data from all buckets.
        settings.path.push_back(Substream::ArrayElements);
        ++settings.array_level;
        const auto & nested_data_serialization = assert_cast<const SerializationArray &>(*nested_serialization).getNestedSerialization();
        auto array_elements_next_data = SubstreamData(nested_data_serialization)
            .withType(map_type ? map_type->getNestedDataType() : nullptr)
            .withColumn(map_column ? map_column->getNestedColumn().getDataPtr() : nullptr);
        auto nested_creator = std::make_shared<SerializationArray::SubcolumnCreator>(map_column ? map_column->getNestedColumn().getOffsetsPtr() : nullptr);
        settings.path.back().creator = std::make_shared<KeysOrValuesSubcolumnCreator>(nested_creator, serialization_version);
        nested_data_serialization->enumerateStreams(settings, callback, array_elements_next_data);
        --settings.array_level;
        settings.path.pop_back();
    }

    /// Need either a deserialization state (reading) or a column (writing) to determine the bucket count.
    if (!map_deserialize_state && !map_column)
        return;

    /// When reading, take the bucket count from the stored state. When writing, calculate it from column statistics.
    size_t buckets = 1;
    if (map_deserialize_state)
        buckets = checkAndGetState<DeserializeBinaryBulkStateBucketsInfo>(map_deserialize_state->buckets_info_state)->buckets;
    else if (map_column)
        buckets = calculateNumberOfBuckets(map_column->getOrCalculateStatistics(), settings.max_buckets_in_map, settings.map_buckets_strategy, settings.map_buckets_coefficient, settings.map_buckets_min_avg_size);

    /// Enumerate a nested Array(Tuple(K, V)) stream for each bucket.
    for (size_t bucket = 0; bucket < buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        next_data.deserialize_state = map_deserialize_state ? map_deserialize_state->bucket_nested_states[bucket] : nullptr;
        nested_serialization->enumerateStreams(settings, callback, next_data);
        settings.path.pop_back();
    }
}

void SerializationMap::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// BASIC format delegates directly to the nested Array(Tuple(K, V)) serialization.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        nested_serialization->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, state);
        return;
    }

    auto map_state = std::make_shared<SerializeBinaryBulkStateMapWithBuckets>();

    /// Open the buckets info stream and write the header.
    settings.path.push_back(Substream::MapBucketsInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map buckets info during serialization of binary bulk state prefix");

    writeBinaryLittleEndian(static_cast<UInt8>(BucketsInfoSerializationVersion::V1), *stream);

    /// Determine the number of buckets from column statistics and write it to the stream.
    UInt64 buckets = 1;
    ColumnMap::StatisticsPtr statistics = assert_cast<const ColumnMap &>(column).getOrCalculateStatistics();
    buckets = calculateNumberOfBuckets(statistics, settings.max_buckets_in_map, settings.map_buckets_strategy, settings.map_buckets_coefficient, settings.map_buckets_min_avg_size);
    writeBinaryLittleEndian(buckets, *stream);
    map_state->buckets = buckets;

    /// Write statistics (average map size and element count) immediately in the prefix.
    if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::PREFIX)
    {
        writeBinary(true, *stream);
        writeBinaryLittleEndian(statistics->avg, *stream);
        writeBinaryLittleEndian(statistics->count, *stream);
    }
    /// Write a false flag as a placeholder without actual statistics.
    /// Used in Compact parts where the prefix is written for each granule,
    /// but statistics are only needed from the first granule during merge.
    else if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::PREFIX_EMPTY)
    {
        writeBinary(false, *stream);
    }
    /// Defer writing to `serializeBinaryBulkStateSuffix` after the actual data has been processed.
    else if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::SUFFIX)
    {
        map_state->recalculate_statistics = true;
    }

    /// Initialize nested serialization state for each bucket sub-stream.
    map_state->bucket_nested_states.resize(buckets);
    for (size_t bucket = 0; bucket < buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        nested_serialization->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, map_state->bucket_nested_states[bucket]);
        settings.path.pop_back();
    }

    state = std::move(map_state);
}

void SerializationMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// BASIC format delegates directly to the nested Array(Tuple(K, V)) serialization.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
        return;
    }

    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMapWithBuckets>(state);

    /// In SUFFIX mode the statistics were accumulated during `serializeBinaryBulkWithMultipleStreams`.
    /// Now append them to the buckets info stream after all the data has been written.
    if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::SUFFIX)
    {
        settings.path.push_back(Substream::MapBucketsInfo);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map buckets info during serialization of binary bulk state suffix");

        writeBinary(true, *stream);
        writeBinaryLittleEndian(map_state->statistics.avg, *stream);
        writeBinaryLittleEndian(map_state->statistics.count, *stream);
    }

    /// Finalize nested serialization state for each bucket sub-stream.
    for (size_t bucket = 0; bucket < map_state->buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        nested_serialization->serializeBinaryBulkStateSuffix(settings, map_state->bucket_nested_states[bucket]);
        settings.path.pop_back();
    }
}

ISerialization::DeserializeBinaryBulkStatePtr
SerializationMap::deserializeBucketsInfoStatePrefix(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache)
{
    settings.path.push_back(Substream::MapBucketsInfo);

    DeserializeBinaryBulkStatePtr state = nullptr;
    /// Try to reuse a previously deserialized state from the cache.
    /// This happens when multiple columns share the same buckets info stream
    /// (e.g. keys and values subcolumns of the same Map).
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = std::move(cached_state);
    }
    /// Otherwise read the buckets info stream from disk.
    else if (auto * stream = settings.getter(settings.path))
    {
        UInt8 version;
        readBinary(version, *stream);
        if (!magic_enum::enum_cast<BucketsInfoSerializationVersion>(version))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Map buckets info serialization version: {}", static_cast<UInt32>(version));

        /// Read number of buckets.
        UInt64 buckets;
        readBinaryLittleEndian(buckets, *stream);

        /// Read statistics if any.
        ColumnMap::StatisticsPtr statistics;
        bool have_statistics = false;
        readBinary(have_statistics, *stream);
        if (have_statistics)
        {
            auto mutable_statistics = std::make_shared<ColumnMap::Statistics>();
            readBinaryLittleEndian(mutable_statistics->avg, *stream);
            readBinaryLittleEndian(mutable_statistics->count, *stream);
            statistics = std::move(mutable_statistics);
        }

        state = std::make_shared<DeserializeBinaryBulkStateBucketsInfo>(buckets, statistics);
        /// Add deserialized state to the cache.
        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }

    settings.path.pop_back();
    return state;
}

ISerialization::DeserializeBinaryBulkStatePtr
SerializationMap::deserializeMapReadingInfoStatePrefix(SubstreamsDeserializeStatesCache * cache, const ISerialization::SubstreamPath & path)
{
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, path))
        return cached_state;

    auto new_state = std::make_shared<DeserializeBinaryBulkStateMapReadingInfo>();
    addToSubstreamsDeserializeStatesCache(cache, path, new_state);
    return new_state;
}

void SerializationMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto map_state = std::make_shared<DeserializeBinaryBulkStateMap>();

    /// Create or get the shared reading info state cached at the current substream path.
    /// Set the flag so that `SerializationMapKeyValue` knows the full Map is being read
    /// and must keep the intermediate nested column for cache sharing.
    map_state->reading_info_state = deserializeMapReadingInfoStatePrefix(cache, settings.path);
    checkAndGetState<DeserializeBinaryBulkStateMapReadingInfo>(map_state->reading_info_state)->reading_full_map = true;

    /// BASIC format stores the Map as a plain Array(Tuple(K, V)) — no bucketing.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        nested_serialization->deserializeBinaryBulkStatePrefix(settings, map_state->nested_state, cache);
        state = std::move(map_state);
        return;
    }

    /// Read the bucket count and optional statistics from the buckets info stream.
    map_state->buckets_info_state = deserializeBucketsInfoStatePrefix(settings, cache);
    const auto * buckets_info_state_concrete = checkAndGetState<DeserializeBinaryBulkStateBucketsInfo>(map_state->buckets_info_state);

    /// Initialize nested deserialization state for each bucket sub-stream.
    map_state->bucket_nested_states.resize(buckets_info_state_concrete->buckets);
    for (size_t bucket = 0; bucket < buckets_info_state_concrete->buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        nested_serialization->deserializeBinaryBulkStatePrefix(settings, map_state->bucket_nested_states[bucket], cache);
        settings.path.pop_back();
    }

    state = std::move(map_state);
}

namespace
{

template <typename KeyColumn>
size_t getBucketForKeyImpl(const KeyColumn & key_column, size_t row, size_t buckets)
{
    if (buckets == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of buckets cannot be zero");

    SipHash hash;
    key_column.updateHashWithValue(row, hash);
    return hash.get64() % buckets;
}

}

size_t SerializationMap::getBucketForKey(const ColumnPtr & key_column, size_t row, size_t buckets)
{
    return getBucketForKeyImpl<IColumn>(*key_column, row, buckets);
}

size_t SerializationMap::calculateNumberOfBuckets(const ColumnMap::StatisticsPtr & statistics, size_t max_buckets, MergeTreeMapBucketsStrategy strategy, double coefficient, size_t min_avg_size)
{
    /// If a minimum average map size threshold is set and the average map size is below it,
    /// use a single bucket to avoid bucketing overhead for small maps.
    if (min_avg_size > 0 && statistics->avg < static_cast<Float64>(min_avg_size))
        return 1;

    UInt64 result;
    switch (strategy)
    {
        /// Always use max_buckets_in_map regardless of the average map size.
        case MergeTreeMapBucketsStrategy::CONSTANT:
            result = max_buckets;
            break;
        /// Use round(coefficient * sqrt(avg_map_size)), good for sublinear growth of buckets.
        case MergeTreeMapBucketsStrategy::SQRT:
            result = static_cast<UInt64>(round(coefficient * sqrt(statistics->avg)));
            break;
        /// Use round(coefficient * avg_map_size), buckets grow proportionally to the average map size.
        case MergeTreeMapBucketsStrategy::LINEAR:
            result = static_cast<UInt64>(round(coefficient * statistics->avg));
            break;
    }
    return std::min(std::max(result, static_cast<UInt64>(1)), static_cast<UInt64>(max_buckets));
}

namespace
{

/// Dispatch macro for devirtualizing IColumn operations on common Map key/value types.
/// For each supported TypeIndex, the concrete column type is passed to CALL,
/// enabling the compiler to inline insertFrom/insertRangeFrom/updateHashWithValue
/// instead of going through virtual dispatch. Falls back to IColumn in generic case.
// clang-format off
#define DISPATCH_MAP_COLUMN_TYPE(type_index, CALL) \
    switch (type_index) \
    { \
        case TypeIndex::UInt8: { CALL(ColumnVector<UInt8>); break; } \
        case TypeIndex::UInt16: { CALL(ColumnVector<UInt16>); break; } \
        case TypeIndex::UInt32: { CALL(ColumnVector<UInt32>); break; } \
        case TypeIndex::UInt64: { CALL(ColumnVector<UInt64>); break; } \
        case TypeIndex::Int8: { CALL(ColumnVector<Int8>); break; } \
        case TypeIndex::Int16: { CALL(ColumnVector<Int16>); break; } \
        case TypeIndex::Int32: { CALL(ColumnVector<Int32>); break; } \
        case TypeIndex::Int64: { CALL(ColumnVector<Int64>); break; } \
        case TypeIndex::Float32: { CALL(ColumnVector<Float32>); break; } \
        case TypeIndex::Float64: { CALL(ColumnVector<Float64>); break; } \
        case TypeIndex::String: { CALL(ColumnString); break; } \
        case TypeIndex::FixedString: { CALL(ColumnFixedString); break; } \
        default: { CALL(IColumn); break; } \
    }
// clang-format on

/// Devirtualized inner loop for splitting a Map column into buckets.
/// This is the innermost function in the two-level dispatch chain (key type -> value type).
/// All casts are performed upfront: source key/value columns and every
/// destination bucket column are cast to their concrete types before entering the hot loop,
/// so that `insertFrom` and `getBucketForKeyImpl` are resolved at compile time.
/// We use `static_cast` here because the dispatch macro guarantees the correct type,
/// and `assert_cast` would fail in debug builds for the `IColumn` fallback case
/// (it requires exact `typeid` match, but the runtime type is a concrete column).
template <typename KeyColumn, typename ValueColumn>
void splitMapToBucketsTyped(
    const IColumn & src_keys_col,
    const IColumn & src_values_col,
    const ColumnArray::Offsets & src_offsets,
    std::vector<IColumn *> & dst_keys_raw,
    std::vector<IColumn *> & dst_values_raw,
    std::vector<ColumnArray::Offsets *> & dst_offsets,
    size_t start, size_t end, size_t num_buckets)
{
    const auto & src_keys = static_cast<const KeyColumn &>(src_keys_col);
    const auto & src_values = static_cast<const ValueColumn &>(src_values_col);

    std::vector<KeyColumn *> dst_keys(num_buckets);
    std::vector<ValueColumn *> dst_values(num_buckets);
    for (size_t b = 0; b < num_buckets; ++b)
    {
        dst_keys[b] = static_cast<KeyColumn *>(dst_keys_raw[b]);
        dst_values[b] = static_cast<ValueColumn *>(dst_values_raw[b]);
    }

    for (size_t i = start; i != end; ++i)
    {
        size_t offset_start = src_offsets[ssize_t(i) - 1];
        size_t offset_end = src_offsets[ssize_t(i)];
        for (size_t j = offset_start; j != offset_end; ++j)
        {
            size_t bucket = getBucketForKeyImpl(src_keys, j, num_buckets);
            dst_keys[bucket]->insertFrom(src_keys, j);
            dst_values[bucket]->insertFrom(src_values, j);
        }

        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
            dst_offsets[bucket]->push_back(dst_keys[bucket]->size());
    }
}

/// Second level of the two-level type dispatch for `splitMapToBuckets`.
/// The key column type is already fixed as `KeyColumn`; this function dispatches
/// on the value column `TypeIndex` and calls `splitMapToBucketsTyped<KeyColumn, ValueColumn>`.
template <typename KeyColumn>
void splitMapToBucketsDispatchByValue(
    const IColumn & src_keys, const IColumn & src_values,
    const ColumnArray::Offsets & src_offsets,
    std::vector<IColumn *> & dst_keys, std::vector<IColumn *> & dst_values,
    std::vector<ColumnArray::Offsets *> & dst_offsets,
    size_t start, size_t end, size_t num_buckets)
{
#define CALL_SPLIT(ValueColumn) splitMapToBucketsTyped<KeyColumn, ValueColumn>( \
    src_keys, src_values, src_offsets, dst_keys, dst_values, dst_offsets, start, end, num_buckets)
    DISPATCH_MAP_COLUMN_TYPE(src_values.getDataType(), CALL_SPLIT)
#undef CALL_SPLIT
}

/// Entry point of the two-level type dispatch for splitting a Map column into buckets.
/// Dispatches on the key column `TypeIndex`, then delegates to
/// `splitMapToBucketsDispatchByValue` which dispatches on the value column type.
/// For recognized concrete types (integer/float `ColumnVector` variants, `ColumnString`,
/// `ColumnFixedString`) the call resolves to a fully devirtualized `splitMapToBucketsTyped`;
/// for any other type it falls back to the `IColumn` interface (virtual dispatch).
void splitMapToBucketsDispatch(
    const IColumn & src_keys, const IColumn & src_values,
    const ColumnArray::Offsets & src_offsets,
    std::vector<IColumn *> & dst_keys, std::vector<IColumn *> & dst_values,
    std::vector<ColumnArray::Offsets *> & dst_offsets,
    size_t start, size_t end, size_t num_buckets)
{
#define CALL_KEY_SPLIT(KeyColumn) splitMapToBucketsDispatchByValue<KeyColumn>( \
    src_keys, src_values, src_offsets, dst_keys, dst_values, dst_offsets, start, end, num_buckets)
    DISPATCH_MAP_COLUMN_TYPE(src_keys.getDataType(), CALL_KEY_SPLIT)
#undef CALL_KEY_SPLIT
}

#undef DISPATCH_MAP_COLUMN_TYPE

}


/// Splits a range of rows [start, end) from a Map column into `buckets` separate Map columns,
/// one per bucket. Each key-value pair is assigned to a bucket by hashing the key via
/// `getBucketForKeyImpl`. Uses two-level type dispatch (`splitMapToBucketsDispatch`) to
/// devirtualize `insertFrom` and hash computation for common key/value column types.
VectorWithMemoryTracking<ColumnPtr> SerializationMap::splitMapToBuckets(const IColumn & map_column, size_t start, size_t end, size_t buckets) const
{
    VectorWithMemoryTracking<ColumnPtr> map_buckets(buckets);
    std::vector<IColumn *> map_keys_buckets(buckets);
    std::vector<IColumn *> map_values_buckets(buckets);
    std::vector<ColumnArray::Offsets *> map_offsets_buckets(buckets);
    for (size_t bucket = 0; bucket != buckets; ++bucket)
    {
        auto column = map_column.cloneEmpty();
        auto & nested_column = assert_cast<ColumnMap &>(*column).getNestedColumn();
        auto & nested_data = assert_cast<ColumnTuple &>(nested_column.getData());
        map_offsets_buckets[bucket] = &nested_column.getOffsets();
        map_keys_buckets[bucket] = &nested_data.getColumn(0);
        map_values_buckets[bucket] = &nested_data.getColumn(1);
        map_buckets[bucket] = std::move(column);
    }

    const auto & nested_column = assert_cast<const ColumnMap &>(map_column).getNestedColumn();
    const auto & nested_data = assert_cast<const ColumnTuple &>(nested_column.getData());
    const auto & map_keys_column = nested_data.getColumnPtr(0);
    const auto & map_values_column = nested_data.getColumnPtr(1);
    const auto & map_offsets = nested_column.getOffsets();

    splitMapToBucketsDispatch(
        *map_keys_column, *map_values_column, map_offsets,
        map_keys_buckets, map_values_buckets, map_offsets_buckets,
        start, end, buckets);

    return map_buckets;
}

/// Reassembles a single Map column from per-bucket Map columns produced by `splitMapToBuckets`.
/// For each row, key-value pairs from all buckets are concatenated back into one map entry.
/// Used during deserialization to reconstruct the original Map column from the bucketed
/// on-disk representation.
void SerializationMap::collectMapFromBuckets(const VectorWithMemoryTracking<ColumnPtr> & map_buckets, IColumn & map_column) const
{
    if (map_buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty list of buckets provided");

    VectorWithMemoryTracking<ColumnPtr> map_keys_buckets(map_buckets.size());
    VectorWithMemoryTracking<ColumnPtr> map_values_buckets(map_buckets.size());
    std::vector<const ColumnArray::Offsets *> map_offsets_buckets(map_buckets.size());
    for (size_t bucket = 0; bucket != map_buckets.size(); ++bucket)
    {
        const auto & nested_column = assert_cast<const ColumnMap &>(*map_buckets[bucket]).getNestedColumn();
        const auto & nested_data = assert_cast<const ColumnTuple &>(nested_column.getData());
        map_offsets_buckets[bucket] = &nested_column.getOffsets();
        map_keys_buckets[bucket] = nested_data.getColumnPtr(0);
        map_values_buckets[bucket] = nested_data.getColumnPtr(1);
    }

    auto & nested_column = assert_cast<ColumnMap &>(map_column).getNestedColumn();
    auto & nested_data = assert_cast<ColumnTuple &>(nested_column.getData());
    auto & map_keys_column = nested_data.getColumn(0);
    auto & map_values_column = nested_data.getColumn(1);
    auto & map_offsets = nested_column.getOffsets();
    size_t num_rows = map_buckets[0]->size();
    map_offsets.reserve(map_offsets.size() + num_rows);
    map_keys_column.prepareForSquashing(map_keys_buckets, 1);
    map_values_column.prepareForSquashing(map_values_buckets, 1);
    for (size_t i = 0; i != num_rows; ++i)
    {
        for (size_t bucket = 0; bucket != map_buckets.size(); ++bucket)
        {
            size_t offset_start = (*map_offsets_buckets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*map_offsets_buckets[bucket])[ssize_t(i)];
            map_keys_column.insertRangeFrom(*map_keys_buckets[bucket], offset_start, offset_end - offset_start);
            map_values_column.insertRangeFrom(*map_values_buckets[bucket], offset_start, offset_end - offset_start);
        }

        map_offsets.push_back(map_keys_column.size());
    }
}

void SerializationMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// BASIC format delegates directly to the nested Array(Tuple(K, V)) serialization.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        nested_serialization->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
        return;
    }

    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMapWithBuckets>(state);
    size_t end = limit && offset + limit < column.size() ? offset + limit : column.size();

    /// Fast path for a single bucket. No splitting needed, serialize the column directly.
    if (map_state->buckets == 1)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = 0;
        nested_serialization->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, map_state->bucket_nested_states[0]);
        settings.path.pop_back();
    }
    /// Multiple buckets. Split the Map column by key hash, then serialize each bucket independently.
    else
    {
        auto map_buckets = splitMapToBuckets(column, offset, end, map_state->buckets);
        for (size_t bucket = 0; bucket < map_state->buckets; ++bucket)
        {
            settings.path.push_back(SubstreamType::Bucket);
            settings.path.back().bucket = bucket;
            nested_serialization->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(*map_buckets[bucket]), 0, map_buckets[bucket]->size(), settings, map_state->bucket_nested_states[bucket]);
            settings.path.pop_back();
        }
    }

    /// Accumulate statistics from each serialized range.
    /// They will be written to the stream in `serializeBinaryBulkStateSuffix`.
    if (map_state->recalculate_statistics)
        map_state->statistics.merge(*assert_cast<const ColumnMap &>(column).calculateStatisticsForRange(offset, end));
}

void SerializationMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    auto & column_map = assert_cast<ColumnMap &>(*mutable_column);

    if (!state)
        return;

    auto * map_state = checkAndGetState<DeserializeBinaryBulkStateMap>(state);

    /// BASIC format delegates directly to the nested Array(Tuple(K, V)) serialization.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        ColumnPtr nested_ptr = column_map.getNestedColumnPtr();
        /// Try to get data from the substreams cache before reading from disk.
        if (!insertDataFromSubstreamsCacheIfAny(cache, settings, nested_ptr))
        {
            size_t prev_size = column->size();
            nested_serialization->deserializeBinaryBulkWithMultipleStreams(nested_ptr, rows_offset, limit, settings, map_state->nested_state, cache);
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, nested_ptr, nested_ptr->size() - prev_size);
        }

        /// Write back: nested_ptr may have been reassigned to a cached column
        /// by insertDataFromSubstreamsCacheIfAny or by the nested deserialization.
        /// Without this, the Map column would remain empty while its data lives
        /// only in the local nested_ptr variable.
        column_map.getNestedColumnPtr() = std::move(nested_ptr);
        return;
    }

    auto * buckets_info_state = checkAndGetState<DeserializeBinaryBulkStateBucketsInfo>(map_state->buckets_info_state);

    /// Fast path for a single bucket. No collecting needed, deserialize directly into the column.
    if (buckets_info_state->buckets == 1)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = 0;

        ColumnPtr nested_ptr = column_map.getNestedColumnPtr();
        /// Try to get data from the substreams cache before reading from disk.
        if (!insertDataFromSubstreamsCacheIfAny(cache, settings, nested_ptr))
        {
            size_t prev_size = column->size();
            nested_serialization->deserializeBinaryBulkWithMultipleStreams(nested_ptr, rows_offset, limit, settings, map_state->bucket_nested_states[0], cache);
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, nested_ptr, nested_ptr->size() - prev_size);
        }

        /// Write back: nested_ptr may have been reassigned (same reason as BASIC path above).
        column_map.getNestedColumnPtr() = std::move(nested_ptr);
        settings.path.pop_back();
    }
    /// Multiple buckets. Deserialize each bucket into a separate Map column,
    /// then reassemble them into a single column via `collectMapFromBuckets`.
    else
    {
        VectorWithMemoryTracking<ColumnPtr> map_buckets(buckets_info_state->buckets);
        for (size_t bucket = 0; bucket != buckets_info_state->buckets; ++bucket)
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            map_buckets[bucket] = column_map.cloneEmpty();
            ColumnPtr nested_ptr = assert_cast<const ColumnMap &>(*map_buckets[bucket]).getNestedColumnPtr();
            nested_serialization->deserializeBinaryBulkWithMultipleStreams(nested_ptr, rows_offset, limit, settings, map_state->bucket_nested_states[bucket], cache);
            settings.path.pop_back();
        }

        collectMapFromBuckets(map_buckets, column_map);
    }
}

}
