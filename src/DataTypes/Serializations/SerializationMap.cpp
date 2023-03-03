#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeMap.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/WeakHash.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
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
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
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

        key_writer(ostr, key, nested_tuple.getColumn(0), i);
        writeChar(':', ostr);
        value_writer(ostr, value, nested_tuple.getColumn(1), i);
    }
    writeChar('}', ostr);
}

template <typename Reader>
void SerializationMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader) const
{
    auto & column_map = assert_cast<ColumnMap &>(column);

    auto & nested_array = column_map.getNestedColumn();
    auto & nested_tuple = column_map.getNestedData();
    auto & offsets = nested_array.getOffsets();

    auto & key_column = nested_tuple.getColumn(0);
    auto & value_column = nested_tuple.getColumn(1);

    size_t size = 0;
    assertChar('{', istr);

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                    ++istr.position();
                else
                    throw Exception(ErrorCodes::CANNOT_READ_MAP_FROM_TEXT, "Cannot read Map from text");
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            reader(istr, key, key_column);
            ++size;

            skipWhitespaceIfAny(istr);
            assertChar(':', istr);
            skipWhitespaceIfAny(istr);

            reader(istr, value, value_column);

            skipWhitespaceIfAny(istr);
        }

        assertChar('}', istr);
    }
    catch (...)
    {
        if (size)
        {
            nested_tuple.getColumnPtr(0) = key_column.cut(0, offsets.back());
            nested_tuple.getColumnPtr(1) = value_column.cut(0, offsets.back());
        }
        throw;
    }

    offsets.push_back(offsets.back() + size);
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
            subcolumn_serialization->deserializeTextQuoted(subcolumn, buf, settings);
        });

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Map");
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

void SerializationMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
        {
            if (settings.null_as_default)
                SerializationNullable::deserializeTextJSONImpl(subcolumn, buf, settings, subcolumn_serialization);
            else
                subcolumn_serialization->deserializeTextJSON(subcolumn, buf, settings);
        });
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

        if (size == 1)
        {
            /// shortcut for empty string
            shard_chars[chars_positions[pos]++] = 0;
            shard_offsets[offsets_positions[pos]++] = shard_chars.size();
        }
        else
        {
            const size_t src_offset = src_offsets[i - 1];

            memcpySmallAllowReadWriteOverflow15(shard_chars.data() + chars_positions[pos], &src_chars[src_offset], size);

            chars_positions[pos] += size;
            shard_offsets[offsets_positions[pos]++] = chars_positions[pos];
        }
    }

    return scattered_columns;
}

MutableColumns scatterColumn(
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

    UInt64 prev_offset = 0;
    auto & selector = hash.getData();
    std::vector<UInt64> current_shard_array_offset(num_shards);

    auto fill_selector = [&](auto && sharder)
    {
        for (size_t i = 0; i < map_offsets.size(); ++i)
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

SerializationPtr SerializationMap::SubcolumnCreator::create(const SerializationPtr & prev) const
{
    return std::make_shared<SerializationNamed>(prev, shard_name, false);
}

void SerializationMap::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        auto next_data = SubstreamData(nested)
            .withType(data.type ? assert_cast<const DataTypeMap &>(*data.type).getNestedType() : nullptr)
            .withColumn(data.column ? assert_cast<const ColumnMap &>(*data.column).getNestedColumnPtr() : nullptr)
            .withSerializationInfo(data.serialization_info);

        nested->enumerateStreams(settings, callback, next_data);
    }

    if (num_shards > 1 && settings.type_map_enumerate_shards)
    {
        auto shard_serialization = std::make_shared<SerializationMap>(key, value, nested, 1);

        DataTypePtr shard_type;
        if (data.type)
            shard_type = std::make_shared<DataTypeMap>(assert_cast<const DataTypeMap &>(*data.type).getNestedType(), 1);

        std::vector<ColumnPtr> shard_columns;
        if (data.column)
            shard_columns = scatterToShards(*data.column, num_shards);

        auto next_data = SubstreamData(nested)
            .withType(data.type ? assert_cast<const DataTypeMap &>(*data.type).getNestedType() : nullptr)
            .withSerializationInfo(data.serialization_info);

        settings.path.push_back(ISerialization::Substream::MapShard);
        for (size_t i = 0; i < num_shards; ++i)
        {
            auto shard_name = "shard" + toString(i);
            auto shard_named = std::make_shared<SerializationNamed>(shard_serialization, shard_name, false);
            auto shard_data = SubstreamData(shard_named)
                .withType(shard_type)
                .withColumn(data.column ? ColumnMap::create(shard_columns[i]) : nullptr)
                .withSerializationInfo(data.serialization_info);

            settings.path.back().visited = false;
            settings.path.back().map_shard_num = i;
            settings.path.back().data = std::move(shard_data);

            if (data.column)
                next_data.withColumn(shard_columns[i]);

            settings.path.back().creator = std::make_shared<SubcolumnCreator>(shard_name);
            nested->enumerateStreams(settings, callback, next_data);
        }
        settings.path.pop_back();
    }
}

void SerializationMap::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, state);
}

void SerializationMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state);
}

void SerializationMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        nested->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
        return;
    }

    auto shards = scatterToShards(column, num_shards);
    settings.path.push_back(ISerialization::Substream::MapShard);

    for (size_t i = 0; i < num_shards; ++i)
    {
        settings.path.back().map_shard_num = i;
        nested->serializeBinaryBulkWithMultipleStreams(*shards[i], offset, limit, settings, state);
    }

    settings.path.pop_back();
}

void SerializationMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (num_shards == 1 || !settings.position_independent_encoding)
    {
        auto & column_map = assert_cast<ColumnMap &>(*column->assumeMutable());
        nested->deserializeBinaryBulkWithMultipleStreams(column_map.getNestedColumnPtr(), limit, settings, state, cache);
        return;
    }

    auto mutable_column = column->assumeMutable();
    auto & column_map = assert_cast<ColumnMap &>(*mutable_column);
    auto & column_nested = column_map.getNestedColumn();

    std::vector<ColumnPtr> shard_arrays(num_shards);
    settings.path.push_back(ISerialization::Substream::MapShard);

    for (size_t i = 0; i < num_shards; ++i)
    {
        settings.path.back().map_shard_num = i;
        shard_arrays[i] = column_nested.cloneEmpty();
        nested->deserializeBinaryBulkWithMultipleStreams(shard_arrays[i], limit, settings, state, cache);
    }
    settings.path.pop_back();

    std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
    {
        const auto & shard_array = assert_cast<const ColumnArray &>(*shard_arrays[i]);
        sources[i] = GatherUtils::createArraySource(shard_array, false, shard_array.size());
    }

    GatherUtils::concatInplace(sources, column_nested);
    column = std::move(mutable_column);
}

}
