#include <DataTypes/Serializations/SerializationCompactNullMap.h>

#include <Columns/ColumnsCommon.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}

SerializationPtr SerializationCompactNullMap::create()
{
    SipHash hash;
    hash.update("CompactNullMap");
    return ISerialization::pooled(hash.get128(), [] { return new SerializationCompactNullMap(); });
}

void SerializationCompactNullMap::serializeGranule(const PaddedPODArray<UInt8> * null_map, size_t offset, size_t limit, WriteBuffer & ostr)
{
    if (limit == 0)
        return;

    GranuleFormat format = NO_NULLS;
    if (null_map && !memoryIsZero(null_map->data(), offset, offset + limit))
        format = memoryIsByte(null_map->data(), offset, offset + limit, 1) ? ALL_NULLS : PLAIN;

    writeVarUInt(limit, ostr);
    writeBinaryLittleEndian(static_cast<UInt8>(format), ostr);
    if (format == PLAIN)
        SerializationNumber<UInt8>::serializeBinaryBulk(*null_map, ostr, offset, limit);
}

std::pair<size_t, ColumnUInt8::MutablePtr> SerializationCompactNullMap::deserializeRows(
    DeserializeBinaryBulkStateCompactNullMap & state, ReadBuffer & istr, size_t limit, bool continuous_reading)
{
    /// Reset the state if we are reading from the start of a granule and not from the previous position in the stream.
    if (!continuous_reading)
        state.remaining_rows_in_granule = 0;

    size_t num_read_rows = 0;
    /// Created when the first NULL is read.
    ColumnUInt8::MutablePtr null_map;

    while (limit)
    {
        if (state.remaining_rows_in_granule == 0)
        {
            if (istr.eof())
                break;

            UInt64 granule_size = 0;
            readVarUInt(granule_size, istr);
            UInt8 format = 0;
            readBinaryLittleEndian(format, istr);
            if (granule_size == 0 || format > PLAIN)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Incorrect granule of the compact null map: {} rows, format {}", granule_size, static_cast<UInt32>(format));

            state.remaining_rows_in_granule = granule_size;
            state.granule_format = static_cast<GranuleFormat>(format);
        }

        size_t limit_in_granule = std::min(limit, state.remaining_rows_in_granule);

        if (state.granule_format != NO_NULLS && !null_map)
        {
            null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(num_read_rows, 0);
        }

        if (null_map)
        {
            auto & data = null_map->getData();
            if (state.granule_format == PLAIN)
            {
                size_t prev_size = data.size();
                SerializationNumber<UInt8>::deserializeBinaryBulk(data, istr, limit_in_granule);
                if (data.size() - prev_size != limit_in_granule)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data of the compact null map. Expected: {}, got: {}", limit_in_granule, data.size() - prev_size);
            }
            else
            {
                data.resize_fill(data.size() + limit_in_granule, state.granule_format == ALL_NULLS ? 1 : 0);
            }
        }

        state.remaining_rows_in_granule -= limit_in_granule;
        limit -= limit_in_granule;
        num_read_rows += limit_in_granule;
    }

    return {num_read_rows, std::move(null_map)};
}

ISerialization::DeserializeBinaryBulkStatePtr SerializationCompactNullMap::deserializeStatePrefix(
    DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache)
{
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
        return cached_state;

    auto state = std::make_shared<DeserializeBinaryBulkStateCompactNullMap>();
    addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    return state;
}

void SerializationCompactNullMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & /*state*/) const
{
    const auto & null_map = assert_cast<const ColumnUInt8 &>(column).getData();
    if (const size_t size = null_map.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    if (auto * stream = settings.getter(settings.path))
        serializeGranule(&null_map, offset, limit, *stream);
}

void SerializationCompactNullMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    state = deserializeStatePrefix(settings, cache);
}

void SerializationCompactNullMap::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        return;

    auto * stream = settings.getter(settings.path);
    if (!stream)
        return;

    auto * null_map_state = checkAndGetState<DeserializeBinaryBulkStateCompactNullMap>(state);

    auto & data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t prev_size = data.size();
    auto [num_read_rows, null_map] = deserializeRows(*null_map_state, *stream, limit, settings.continuous_reading);
    if (null_map)
        data.insert(null_map->getData().begin(), null_map->getData().end());
    else
        data.resize_fill(prev_size + num_read_rows, 0);

    addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), num_read_rows);
}

}
