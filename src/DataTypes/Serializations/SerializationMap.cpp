#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/DataTypeMap.h>

#include <Common/WeakHash.h>
#include <Common/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/BitHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

SerializationMap::SerializationMap(
    const SerializationPtr & key_,
    const SerializationPtr & value_,
    const SerializationPtr & nested_,
    size_t num_shards_)
    : key(key_), value(value_), nested(nested_), num_shards(num_shards_)
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
    const auto & map = field.get<const Map &>();
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple>();
        assert(tuple.size() == 2);
        key->serializeBinary(tuple[0], ostr, settings);
        value->serializeBinary(tuple[1], ostr, settings);
    }
}

void SerializationMap::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    size_t size;
    readVarUInt(size, istr);
    if (settings.max_binary_array_size && size > settings.max_binary_array_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Too large map size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_array_size",
            size,
            settings.max_binary_array_size);

    field = Map();
    Map & map = field.get<Map &>();
    map.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        Tuple tuple(2);
        key->deserializeBinary(tuple[0], istr, settings);
        value->deserializeBinary(tuple[1], istr, settings);
        map.push_back(std::move(tuple));
    }
}

void SerializationMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(extractNestedColumn(column), row_num, ostr, settings);
}

void SerializationMap::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(extractNestedColumn(column), istr, settings);
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

    for (size_t shard = 0; shard < column_map.getNumShards(); ++shard)
    {
        const auto & nested_array = column_map.getShard(shard);
        const auto & nested_tuple = column_map.getShardData(shard);
        const auto & offsets = nested_array.getOffsets();

        size_t offset = offsets[row_num - 1];
        size_t next_offset = offsets[row_num];

        writeChar('{', ostr);
        for (size_t i = offset; i < next_offset; ++i)
        {
            if (i != offset)
                writeChar(',', ostr);

            key_writer(ostr, key, nested_tuple.getColumn(0), i);
            writeChar(':', ostr);
            value_writer(ostr, value, nested_tuple.getColumn(1), i);
        }
        writeChar('}', ostr);

    }
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
                reader(istr, key, key_column);
            else if (!reader(istr, key, key_column))
                return on_error_no_throw();

            ++size;

            skipWhitespaceIfAny(istr);
            if constexpr (throw_exception)
                assertChar(':', istr);
            else if (!checkChar(':', istr))
                return on_error_no_throw();
            skipWhitespaceIfAny(istr);

            if constexpr (throw_exception)
                reader(istr, value, value_column);
            else if (!reader(istr, value, value_column))
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
        key->serializeText(nested_tuple.getColumn(0), i, str_buf, settings);

        writeChar(' ', (indent + 1) * 4, ostr);
        writeJSONString(str_buf.str(), ostr, settings);
        writeCString(": ", ostr);
        value->serializeTextJSONPretty(nested_tuple.getColumn(1), i, ostr, settings, indent + 1);
    }
    writeChar('\n', ostr);
    writeChar(' ', indent * 4, ostr);
    writeChar('}', ostr);
}


void SerializationMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(subcolumn, buf, settings, subcolumn_serialization);
            else
                subcolumn_serialization->deserializeTextJSON(subcolumn, buf, settings);
        });
}

bool SerializationMap::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto reader = [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
    {
        if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
            return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(subcolumn, buf, settings, subcolumn_serialization);
        return subcolumn_serialization->tryDeserializeTextJSON(subcolumn, buf, settings);
    };

    return deserializeTextImpl<bool>(column, istr, reader);
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
        key->serializeTextXML(nested_data.getColumn(0), i, ostr, settings);
        writeCString("</key>", ostr);

        writeCString("<value>", ostr);
        value->serializeTextXML(nested_data.getColumn(1), i, ostr, settings);
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

template <typename T>
static MutableColumns scatterNumeric(
    const ColumnVector<T> & column,
    const IColumn::Selector & selector,
    const std::vector<UInt64> & column_sizes)
{
    size_t num_rows = column.size();
    size_t num_columns = column_sizes.size();

    if (num_rows != selector.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Size of selector: {} doesn't match size of column: {}",
            selector.size(), num_rows);

    std::vector<MutableColumnPtr> scattered_columns(num_columns);
    std::vector<size_t> positions(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        scattered_columns[i] = column.cloneEmpty();
        assert_cast<ColumnVector<T> &>(*scattered_columns[i]).getData().resize(column_sizes[i]);
    }

    const auto & column_data = column.getData();
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto & data = assert_cast<ColumnVector<T> &>(*scattered_columns[selector[i]]).getData();
        data[positions[selector[i]]++] = column_data[i];
    }

    return scattered_columns;
}

static MutableColumns scatterString(
    const ColumnString & column,
    const IColumn::Selector & selector,
    const std::vector<UInt64> & column_sizes)
{
    size_t num_rows = column.size();
    size_t num_columns = column_sizes.size();

    if (num_rows != selector.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Size of selector: {} doesn't match size of column: {}",
            selector.size(), num_rows);

    const auto & src_offsets = column.getOffsets();
    const auto & src_chars = column.getChars();

    std::vector<MutableColumnPtr> scattered_columns(num_columns);
    std::vector<UInt64> chars_sizes(num_columns);

    for (size_t i = 0; i < num_rows; ++i)
    {
        const size_t size = src_offsets[i] - src_offsets[i - 1];
        chars_sizes[selector[i]] += size;
    }

    for (size_t i = 0; i < num_columns; ++i)
    {
        scattered_columns[i] = column.cloneEmpty();

        auto & scattered_string = assert_cast<ColumnString &>(*scattered_columns[i]);
        scattered_string.getOffsets().resize(column_sizes[i]);
        scattered_string.getChars().resize(chars_sizes[i]);
    }

    std::vector<UInt64> offsets_positions(num_columns);
    std::vector<UInt64> chars_positions(num_columns);

    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = selector[i];
        const size_t size = src_offsets[i] - src_offsets[i - 1];

        auto & shard_string = assert_cast<ColumnString &>(*scattered_columns[pos]);
        auto & shard_offsets = shard_string.getOffsets();
        auto & shard_chars = shard_string.getChars();

        /// Shortcut for empty string
        if (size == 1)
            shard_chars[chars_positions[pos]++] = 0;
        else
            memcpySmallAllowReadWriteOverflow15(shard_chars.data() + chars_positions[pos], &src_chars[src_offsets[i - 1]], size);

        chars_positions[pos] += size;
        shard_offsets[offsets_positions[pos]++] = chars_positions[pos];
    }

    return scattered_columns;
}

static MutableColumns scatterColumn(
    const IColumn & column,
    const IColumn::Selector & selector,
    const std::vector<UInt64> & column_sizes)
{
#define DISPATCH(TYPE) \
    if (const auto * column_vector = typeid_cast<const ColumnVector<TYPE> *>(&column)) \
        return scatterNumeric(*column_vector, selector, column_sizes);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (const auto * column_string = typeid_cast<const ColumnString *>(&column))
        return scatterString(*column_string, selector, column_sizes);

    return column.scatter(static_cast<UInt32>(column_sizes.size()), selector);
}

static std::vector<ColumnPtr> scatterToShards(const IColumn & column, size_t num_shards)
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & column_keys = column_map.getNestedData().getColumn(0);
    const auto & column_values = column_map.getNestedData().getColumn(1);
    const auto & map_offsets = column_map.getNestedColumn().getOffsets();

    WeakHash32 hash(column_keys.size());
    column_keys.updateWeakHash32(hash);

    std::vector<MutableColumnPtr> shards_offsets_columns;
    std::vector<IColumn::Offsets *> shards_offsets;

    shards_offsets_columns.reserve(num_shards);
    shards_offsets.reserve(num_shards);

    for (size_t i = 0; i < num_shards; ++i)
    {
        auto offsets_column = ColumnUInt64::create();

        shards_offsets.push_back(&offsets_column->getData());
        shards_offsets_columns.push_back(std::move(offsets_column));
        shards_offsets.back()->resize(column_map.size());
    }

    auto & selector = hash.getData();
    std::vector<UInt64> current_shard_array_offset(num_shards);

    auto fill_selector = [&](auto && sharder)
    {
        UInt64 prev_offset = 0;
        for (size_t i = 0; i < column_map.size(); ++i)
        {
            UInt64 map_size = map_offsets[i] - prev_offset;

            for (size_t row = prev_offset; row < prev_offset + map_size; ++row)
            {
                selector[row] = sharder(selector[row]);
                ++current_shard_array_offset[selector[row]];
            }

            for (size_t shard = 0; shard < num_shards; ++shard)
                (*shards_offsets[shard])[i] = current_shard_array_offset[shard];

            prev_offset += map_size;
        }
    };

    if (isPowerOf2(num_shards))
        fill_selector([num_shards](UInt32 x) -> UInt32 { return x & (num_shards - 1); });
    else
        fill_selector([num_shards](UInt32 x) -> UInt32 { return x % num_shards; });

    auto shard_keys = scatterColumn(column_keys, selector, current_shard_array_offset);
    auto shard_values = scatterColumn(column_values, selector, current_shard_array_offset);

    std::vector<ColumnPtr> shards(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
    {
        auto tuple = ColumnTuple::create(Columns{std::move(shard_keys[i]), std::move(shard_values[i])});
        shards[i] = ColumnArray::create(std::move(tuple), std::move(shards_offsets_columns[i]));
    }

    return shards;
}

void SerializationMap::SubcolumnCreator::create(SubstreamData & data, std::string_view name) const
{
    if (data.serialization)
    {
        if (!shard_name.empty() && name.starts_with(shard_name))
            data.serialization = std::make_shared<SerializationNamed>(data.serialization, shard_name, SubstreamType::MapShard);
        else if (name.starts_with("size"))
            data.serialization = std::make_shared<SerializationMapSize>(num_shards, String(name));
        else if (name.starts_with("keys") || name.starts_with("values"))
            data.serialization = std::make_shared<SerializationMapKeysValues>(data.serialization, num_shards);
    }
}

bool SerializationMap::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    if (!tryReadCSV(s, istr, settings.csv))
        return false;
    ReadBufferFromString rb(s);
    return tryDeserializeText(column, rb, settings, true);
}

void SerializationMap::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    auto next_data = SubstreamData(nested)
        .withType(data.type ? assert_cast<const DataTypeMap &>(*data.type).getNestedType() : nullptr)
        .withColumn(data.column ? assert_cast<const ColumnMap &>(*data.column).getNestedColumnPtr() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(data.deserialize_state);

    if (num_shards == 1)
    {
        nested->enumerateStreams(settings, callback, next_data);
        return;
    }

    if (!settings.position_independent_encoding)
    {
        settings.path.push_back(ISerialization::Substream::Regular);
        settings.path.back().creator = std::make_shared<SubcolumnCreator>("", num_shards);

        nested->enumerateStreams(settings, callback, next_data);
        settings.path.pop_back();
    }

    if (settings.type_map_enumerate_shards)
    {
        auto shard_serialization = std::make_shared<SerializationMap>(key, value, nested, /*num_shards=*/ 1);

        // std::vector<ColumnPtr> shard_columns;
        // if (data.column)
        //     shard_columns = scatterToShards(*data.column, num_shards);

        settings.path.push_back(ISerialization::Substream::MapShard);
        for (size_t i = 0; i < num_shards; ++i)
        {
            auto shard_name = "shard_" + toString(i);
            auto shard_named = std::make_shared<SerializationNamed>(shard_serialization, shard_name, SubstreamType::MapShard);
            auto shard_data = SubstreamData(shard_named)
                .withType(data.type)
                .withColumn(data.column)
                .withSerializationInfo(data.serialization_info)
                .withDeserializeState(data.deserialize_state);

            settings.path.back().visited = false;
            settings.path.back().name_of_substream = shard_name;
            settings.path.back().data = std::move(shard_data);

            // if (data.column)
            //     next_data.withColumn(shard_columns[i]);

            settings.path.back().creator = std::make_shared<SubcolumnCreator>(shard_name, num_shards);
            nested->enumerateStreams(settings, callback, next_data);
        }
        settings.path.pop_back();
    }
}

namespace
{

struct SerializeBinaryBulkStateMap : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> states;
    std::vector<ColumnPtr> cached_shards;
};

struct DeserializeBinaryBulkStateMap : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;
    std::vector<ColumnPtr> cached_shards;

    void resetCachedShards()
    {
        for (auto & shard : cached_shards)
            shard.reset();
    }
};

template <typename Settings, Fn<void(size_t)> Func>
void applyForShards(size_t num_shards, Settings & settings, Func && func)
{
    settings.path.push_back(ISerialization::Substream::MapShard);
    for (size_t i = 0; i < num_shards; ++i)
    {
        settings.path.back().name_of_substream = "shard_" + toString(i);
        func(i);
    }
    settings.path.pop_back();
}

void addShardToSubstreamsCache(
    const SerializationPtr & serialization,
    const ColumnPtr & shard_column,
    const ISerialization::SubstreamPath & path,
    ColumnPtr & cached_shard,
    ISerialization::SubstreamsCache * cache)
{
    if (!cache)
        return;

    if (cached_shard)
        cached_shard->assumeMutableRef().insertRangeFrom(*shard_column, 0, shard_column->size());
    else
        cached_shard = shard_column;

    ISerialization::EnumerateStreamsSettings settings;
    settings.position_independent_encoding = false;

    serialization->enumerateStreams(settings, [&](const auto & subpath)
    {
        if (!subpath.empty())
        {
            auto full_path = path;
            full_path.insert(full_path.end(), subpath.begin(), subpath.end());
            ISerialization::addToSubstreamsCache(cache, full_path, subpath.back().data.column);
        }
    }, ISerialization::SubstreamData(serialization).withColumn(cached_shard));
}

ISerialization::SubstreamsCache getSubstreamCacheForShards(
    const SerializationPtr & serialization,
    const ColumnPtr & old_column,
    const ISerialization::SubstreamPath & path,
    const ISerialization::SubstreamsCache * cache)
{
    if (!cache)
        return {};

    std::unordered_map<String, size_t> shard_substreams_old_sizes;

    ISerialization::EnumerateStreamsSettings settings;
    settings.position_independent_encoding = false;

    serialization->enumerateStreams(settings, [&](const auto & subpath)
    {
        if (!subpath.empty())
        {
            auto full_path = path;
            full_path.insert(full_path.end(), subpath.begin(), subpath.end());

            auto substream_name = ISerialization::getSubcolumnNameForStream(full_path);
            const auto & subcolumn = subpath.back().data.column;
            shard_substreams_old_sizes[substream_name] = subcolumn ? subcolumn->size() : 0;
        }
    }, ISerialization::SubstreamData(serialization).withColumn(old_column));

    ISerialization::SubstreamsCache shard_cache;
    for (const auto & [name, column] : *cache)
    {
        auto it = shard_substreams_old_sizes.find(name);
        if (it != shard_substreams_old_sizes.end() && column->size() >= it->second)
        {
            auto shard_column = column->cut(it->second, column->size() - it->second);
            shard_cache[name] = std::move(shard_column);
        }
    }

    return shard_cache;
}

}

void SerializationMap::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        nested->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, state);
        return;
    }

    auto map_state = std::make_shared<SerializeBinaryBulkStateMap>();
    map_state->states.resize(num_shards);

    applyForShards(num_shards, settings, [&](size_t i)
    {
        nested->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, map_state->states[i]);
    });

    state = std::move(map_state);
}

void SerializationMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        nested->serializeBinaryBulkStateSuffix(settings, state);
        return;
    }

    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMap>(state);
    applyForShards(num_shards, settings, [&](size_t i)
    {
        nested->serializeBinaryBulkStateSuffix(settings, map_state->states[i]);
    });

    map_state->cached_shards.clear();
}

void SerializationMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
        return;
    }

    auto map_state = std::make_shared<DeserializeBinaryBulkStateMap>();
    map_state->states.resize(num_shards);
    map_state->cached_shards.resize(num_shards);

    applyForShards(num_shards, settings, [&](size_t i)
    {
        nested->deserializeBinaryBulkStatePrefix(settings, map_state->states[i], cache);
    });

    state = std::move(map_state);
}

void SerializationMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    size_t num_column_shards = column_map.getNumShards();

    if (num_column_shards != 1 && num_shards != num_column_shards)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Serialization (with {} shards) is incompatible with Map column (with {} shards)",
            num_shards, column_map.getNumShards());

    if (num_shards == 1 && num_column_shards == 1)
    {
        nested->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
        return;
    }

    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMap>(state);

    if (offset == 0)
    {
        if (num_shards != 1 && num_column_shards == 1)
            map_state->cached_shards = scatterToShards(column, num_shards);
        else
            map_state->cached_shards = column_map.getShards();
    }

    applyForShards(num_shards, settings, [&](size_t i)
    {
        nested->serializeBinaryBulkWithMultipleStreams(*map_state->cached_shards[i], offset, limit, settings, map_state->states[i]);
    });
}

void SerializationMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & column_map = assert_cast<ColumnMap &>(*column->assumeMutable());
    if (num_shards != column_map.getNumShards())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Serialization (with {} shards) is incompatible with Map column (with {} shards)",
            num_shards, column_map.getNumShards());

    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        nested->deserializeBinaryBulkWithMultipleStreams(column_map.getShardPtr(0), limit, settings, state, cache);
        return;
    }

    auto * map_state = checkAndGetState<DeserializeBinaryBulkStateMap>(state);

    applyForShards(num_shards, settings, [&](size_t i)
    {
        nested->deserializeBinaryBulkWithMultipleStreams(column_map.getShardPtr(i), limit, settings, map_state->states[i], cache);
    });
}

SerializationMapSubcolumn::SerializationMapSubcolumn(SerializationPtr nested_, size_t num_shards_)
    : SerializationWrapper(nested_), num_shards(num_shards_)
{
    if (num_shards <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Custom serialization for subcolumns are supported for type Map with number of shard more than 1");
}

void SerializationMapSubcolumn::assertSettings(const ISerialization::SerializeBinaryBulkSettings & settings)
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SerializationMapSubcolumn does not support serialization with position independent encoding");
}

void SerializationMapSubcolumn::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (settings.type_map_enumerate_shards)
    {
        applyForShards(num_shards, settings, [&](size_t)
        {
            nested_serialization->enumerateStreams(settings, callback, data);
        });
    }
}

void SerializationMapSubcolumn::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
}

void SerializationMapSubcolumn::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}


void SerializationMapSubcolumn::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationMapSubcolumn::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto map_state = std::make_shared<DeserializeBinaryBulkStateMap>();
    map_state->states.resize(num_shards);
    map_state->cached_shards.resize(num_shards);

    applyForShards(num_shards, settings, [&](size_t i)
    {
        nested_serialization->deserializeBinaryBulkStatePrefix(settings, map_state->states[i], cache);
    });

    state = std::move(map_state);
}

SerializationMapKeysValues::SerializationMapKeysValues(SerializationPtr nested_, size_t num_shards_)
    : SerializationMapSubcolumn(nested_, num_shards_)
{
}

void SerializationMapKeysValues::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    std::vector<ColumnPtr> shards(num_shards);
    auto * map_state = checkAndGetState<DeserializeBinaryBulkStateMap>(state);
    if (column->empty())
        map_state->resetCachedShards();

    applyForShards(num_shards, settings, [&](size_t i)
    {
        shards[i] = column->cloneEmpty();
        auto shard_cache = getSubstreamCacheForShards(nested_serialization, map_state->cached_shards[i], settings.path, cache);

        nested_serialization->deserializeBinaryBulkWithMultipleStreams(shards[i], limit, settings, map_state->states[i], &shard_cache);
        addShardToSubstreamsCache(nested_serialization, shards[i], settings.path, map_state->cached_shards[i], cache);
    });

    std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources(num_shards);

    for (size_t i = 0; i < num_shards; ++i)
    {
        const auto & shard_array = assert_cast<const ColumnArray &>(*shards[i]);
        sources[i] = GatherUtils::createArraySource(shard_array, false, shard_array.size());
    }

    auto res = GatherUtils::concat(sources);
    column->assumeMutableRef().insertRangeFrom(*res, 0, res->size());
}

SerializationMapSize::SerializationMapSize(size_t num_shards_, const String & subcolumn_name_)
    : SerializationMapSubcolumn(std::make_shared<SerializationNamed>(std::make_shared<SerializationNumber<UInt64>>(), subcolumn_name_, SubstreamType::NamedOffsets), num_shards_)
{
}

void SerializationMapSize::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * map_state = checkAndGetState<DeserializeBinaryBulkStateMap>(state);
    if (column->empty())
        map_state->resetCachedShards();

    auto mutable_column = column->assumeMutable();
    auto & result_vector = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
    size_t old_size = result_vector.size();

    applyForShards(num_shards, settings, [&](size_t i)
    {
        ColumnPtr shard = column->cloneEmpty();
        auto shard_cache = getSubstreamCacheForShards(nested_serialization, map_state->cached_shards[i], settings.path, cache);

        nested_serialization->deserializeBinaryBulkWithMultipleStreams(shard, limit, settings, map_state->states[i], &shard_cache);
        addShardToSubstreamsCache(nested_serialization, shard, settings.path, map_state->cached_shards[i], cache);
        auto & shard_vector = assert_cast<ColumnUInt64 &>(shard->assumeMutableRef()).getData();

        if (i == 0)
        {
            result_vector.reserve(old_size + shard_vector.size());
            result_vector.insert_assume_reserved(shard_vector.begin(), shard_vector.end());
        }
        else
        {
            assert(old_size + shard_vector.size() == result_vector.size());
            for (size_t j = 0; j < shard_vector.size(); ++j)
                result_vector[old_size + j] += shard_vector[j];
        }
    });

    column = std::move(mutable_column);
}

}
