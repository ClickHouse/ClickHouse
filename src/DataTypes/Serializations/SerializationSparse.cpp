#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// 2^62, because VarInt supports only values < 2^63.
constexpr auto END_OF_GRANULE_FLAG = 1ULL << 62;

struct DeserializeStateSparse : public ISerialization::DeserializeBinaryBulkState
{
    /// Number of default values, that remain from previous read.
    size_t num_trailing_defaults = 0;
    /// Do we have non-default value after @num_trailing_defaults?
    bool has_value_after_defaults = false;
    ISerialization::DeserializeBinaryBulkStatePtr nested;

    void reset()
    {
        num_trailing_defaults = 0;
        has_value_after_defaults = false;
    }
};

void serializeOffsets(
    const IColumn::Offset * offsets_begin,
    const IColumn::Offset * offsets_end,
    WriteBuffer & ostr,
    size_t start,
    size_t end)
{
    for (const auto * offset = offsets_begin; offset != offsets_end; ++offset)
    {
        size_t group_size = *offset - start;
        writeVarUInt(group_size, ostr);
        start += group_size + 1;
    }

    size_t group_size = start < end ? end - start : 0;
    group_size |= END_OF_GRANULE_FLAG;
    writeVarUInt(group_size, ostr);
}


/// Returns number of read rows.
/// @start is the size of column before reading offsets.
size_t deserializeOffsets(
    IColumn::Offsets & offsets,
    ReadBuffer & istr,
    size_t start,
    size_t limit,
    DeserializeStateSparse & state)
{
    if (limit && state.num_trailing_defaults >= limit)
    {
        state.num_trailing_defaults -= limit;
        return limit;
    }

    /// Just try to guess number of offsets.
    offsets.reserve(offsets.size()
        + static_cast<size_t>(limit * (1.0 - ColumnSparse::DEFAULT_RATIO_FOR_SPARSE_SERIALIZATION)));

    bool first = true;
    size_t total_rows = state.num_trailing_defaults;
    if (state.has_value_after_defaults)
    {
        offsets.push_back(start + state.num_trailing_defaults);
        first = false;

        state.has_value_after_defaults = false;
        state.num_trailing_defaults = 0;
        ++total_rows;
    }

    size_t group_size;
    while (!istr.eof())
    {
        readVarUInt(group_size, istr);

        bool end_of_granule = group_size & END_OF_GRANULE_FLAG;
        group_size &= ~END_OF_GRANULE_FLAG;

        size_t next_total_rows = total_rows + group_size;
        group_size += state.num_trailing_defaults;

        if (limit && next_total_rows >= limit)
        {
            /// If it was not last group in granule,
            /// we have to add current non-default value at further reads.
            state.num_trailing_defaults = next_total_rows - limit;
            state.has_value_after_defaults = !end_of_granule;
            return limit;
        }

        if (end_of_granule)
        {
            state.has_value_after_defaults = false;
            state.num_trailing_defaults = group_size;
        }
        else
        {
            /// If we add value to column for first time in current read,
            /// start from column's current size, because it can have some defaults after last offset,
            /// otherwise just start from previous offset.
            size_t start_of_group = start;
            if (!first && !offsets.empty())
                start_of_group = offsets.back() + 1;
            if (first)
                first = false;

            offsets.push_back(start_of_group + group_size);

            state.num_trailing_defaults = 0;
            state.has_value_after_defaults = false;
            ++next_total_rows;
        }

        total_rows = next_total_rows;
    }

    return total_rows;
}

}

DataTypePtr SerializationSparse::SubcolumnCreator::create(const DataTypePtr & prev) const
{
    return prev;
}

SerializationPtr SerializationSparse::SubcolumnCreator::create(const SerializationPtr & prev) const
{
    return std::make_shared<SerializationSparse>(prev);
}

ColumnPtr SerializationSparse::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnSparse::create(prev, offsets, size);
}

DataTypePtr SerializationSparse::NullMapSubcolumnCreator::create(const DataTypePtr & prev) const
{
    if (!WhichDataType(prev).isUInt8())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Null map should have UInt8 type, got {}", prev->getName());

    return prev;
}

SerializationPtr SerializationSparse::NullMapSubcolumnCreator::create(const SerializationPtr & prev) const
{
    const auto * serialization_named = typeid_cast<const SerializationNamed *>(prev.get());
    if (!serialization_named || serialization_named->getElementName() != "null")
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Null map should have SerializationNamed with name 'null'");

    return std::make_shared<SerializationSparseNullMap>();
}

ColumnPtr SerializationSparse::NullMapSubcolumnCreator::create(const ColumnPtr & prev) const
{
    const auto * column_uint8 = typeid_cast<const ColumnUInt8 *>(prev.get());
    if (!column_uint8)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Null map should have UInt8 column, got {}", prev->getName());

    const auto & offsets_data = assert_cast<const ColumnUInt64 &>(*offsets).getData();
    return column_uint8->createWithOffsets(offsets_data, (*prev)[0], size, /*shift=*/ 1);
}

SerializationSparse::SerializationSparse(const SerializationPtr & nested_)
    : nested(nested_)
{
    if (const auto * nested_nullable = typeid_cast<const SerializationNullable *>(nested.get()))
        nested_non_nullable = nested_nullable->getNested();
}

void SerializationSparse::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * column_sparse = data.column ? &assert_cast<const ColumnSparse &>(*data.column) : nullptr;
    size_t column_size = column_sparse ? column_sparse->size() : 0;

    settings.path.push_back(Substream::SparseOffsets);
    auto offsets_data = SubstreamData(std::make_shared<SerializationNumber<UInt64>>())
        .withType(data.type ? std::make_shared<DataTypeUInt64>() : nullptr)
        .withColumn(column_sparse ? column_sparse->getOffsetsPtr() : nullptr)
        .withSerializationInfo(data.serialization_info);

    settings.path.back().data = offsets_data;
    callback(settings.path);

    settings.path.back() = Substream::SparseElements;

    /// Currently there is only one possible subcolumn in Nullable column -- ".null".
    if (nested_non_nullable)
        settings.path.back().creator = std::make_shared<NullMapSubcolumnCreator>(offsets_data.column, column_size);
    else
        settings.path.back().creator = std::make_shared<SubcolumnCreator>(offsets_data.column, column_size);

    settings.path.back().data = data;

    auto next_data = SubstreamData(nested)
        .withType(data.type)
        .withColumn(column_sparse ? column_sparse->getValuesPtr() : nullptr)
        .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & serialization = getSerializationForMultipleStreams();

    settings.path.push_back(Substream::SparseElements);
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column))
        serialization->serializeBinaryBulkStatePrefix(column_sparse->getValuesColumn(), settings, state);
    else
        serialization->serializeBinaryBulkStatePrefix(column, settings, state);

    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column))
        serializeWithMultipleStreamsSparse(*column_sparse, offset, limit, settings, state);
    else
        serializeWithMultipleStreamsGeneric(column, offset, limit, settings, state);
}

void SerializationSparse::serializeWithMultipleStreamsSparse(
    const ColumnSparse & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    size_t size = column.size();
    const auto & offsets_data = column.getOffsetsData();

    const auto * offsets_begin = offset ? std::lower_bound(offsets_data.begin(), offsets_data.end(), offset) : offsets_data.begin();
    const auto * offsets_end = limit ? std::lower_bound(offsets_data.begin(), offsets_data.end(), offset + limit) : offsets_data.end();

    settings.path.push_back(Substream::SparseOffsets);
    if (auto * stream = settings.getter(settings.path))
    {
        size_t end = limit && offset + limit < size ? offset + limit : size;
        serializeOffsets(offsets_begin, offsets_end, *stream, offset, end);
    }

    if (offsets_begin == offsets_end)
    {
        settings.path.pop_back();
        return;
    }

    size_t values_begin = column.getValueIndex(*offsets_begin);
    size_t values_end = column.getValueIndex(*(offsets_end - 1));
    size_t values_length = values_end - values_begin + 1;

    settings.path.back() = Substream::SparseElements;
    serializeValuesWithMultipleStreams(column.getValuesColumn(), values_begin, values_length, settings, state);
    settings.path.pop_back();
}

void SerializationSparse::serializeWithMultipleStreamsGeneric(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    size_t size = column.size();
    auto offsets_column = DataTypeNumber<IColumn::Offset>().createColumn();
    auto & offsets_data = assert_cast<ColumnVector<IColumn::Offset> &>(*offsets_column).getData();
    column.getIndicesOfNonDefaultRows(offsets_data, offset, limit);

    settings.path.push_back(Substream::SparseOffsets);
    if (auto * stream = settings.getter(settings.path))
    {
        size_t end = limit && offset + limit < size ? offset + limit : size;
        serializeOffsets(offsets_data.begin(), offsets_data.end(), *stream, offset, end);
    }

    if (offsets_data.empty())
    {
        settings.path.pop_back();
        return;
    }

    auto values = column.index(*offsets_column, 0);

    settings.path.back() = Substream::SparseElements;
    serializeValuesWithMultipleStreams(*values, 0, values->size(), settings, state);
    settings.path.pop_back();
}

void SerializationSparse::serializeValuesWithMultipleStreams(
    const IColumn & values,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (nested_non_nullable)
    {
        const auto & values_non_nullable = assert_cast<const ColumnNullable &>(values).getNestedColumn();
        nested_non_nullable->serializeBinaryBulkWithMultipleStreams(values_non_nullable, offset, limit, settings, state);
    }
    else
    {
        nested->serializeBinaryBulkWithMultipleStreams(values, offset, limit, settings, state);
    }
}

void SerializationSparse::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & serialization = getSerializationForMultipleStreams();

    settings.path.push_back(Substream::SparseElements);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    const auto & serialization = getSerializationForMultipleStreams();
    auto state_sparse = std::make_shared<DeserializeStateSparse>();

    settings.path.push_back(Substream::SparseElements);
    serialization->deserializeBinaryBulkStatePrefix(settings, state_sparse->nested);
    settings.path.pop_back();

    state = std::move(state_sparse);
}

void SerializationSparse::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * state_sparse = checkAndGetState<DeserializeStateSparse>(state);

    if (auto cached_column = getFromSubstreamsCache(cache, settings.path))
    {
        column = cached_column;
        return;
    }

    if (!settings.continuous_reading)
        state_sparse->reset();

    auto mutable_column = column->assumeMutable();
    auto & column_sparse = assert_cast<ColumnSparse &>(*mutable_column);
    auto & offsets_data = column_sparse.getOffsetsData();

    size_t old_size = offsets_data.size();

    size_t read_rows = 0;
    settings.path.push_back(Substream::SparseOffsets);
    if (auto * stream = settings.getter(settings.path))
        read_rows = deserializeOffsets(offsets_data, *stream, column_sparse.size(), limit, *state_sparse);

    size_t values_limit = offsets_data.size() - old_size;
    auto & values = column_sparse.getValuesPtr();

    settings.path.back() = Substream::SparseElements;
    deserializeValuesWithMultipleStreams(values, values_limit, settings, state_sparse->nested);
    settings.path.pop_back();

    if (offsets_data.size() + 1 != values->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent sizes of values and offsets in SerializationSparse."
        " Offsets size: {}, values size: {}", offsets_data.size(), values->size());

    /// 'insertManyDefaults' just increases size of column.
    column_sparse.insertManyDefaults(read_rows);
    column = std::move(mutable_column);
    addToSubstreamsCache(cache, settings.path, column);
}

void SerializationSparse::deserializeValuesWithMultipleStreams(
    ColumnPtr & values,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    if (nested_non_nullable)
    {
        auto & values_nullable = assert_cast<ColumnNullable &>(values->assumeMutableRef());
        auto & values_nested = values_nullable.getNestedColumnPtr();
        auto & values_null_map = values_nullable.getNullMapData();

        nested_non_nullable->deserializeBinaryBulkWithMultipleStreams(values_nested, limit, settings, state, nullptr);
        values_null_map.resize_fill(values_null_map.size() + limit, 0);
        return;
    }

    /// Do not use substream cache while reading values column, because ColumnSparse can be cached only in a whole.
    nested->deserializeBinaryBulkWithMultipleStreams(values, limit, settings, state, nullptr);
}

/// All methods below just wrap nested serialization.

void SerializationSparse::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}

void SerializationSparse::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}

void SerializationSparse::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeBinary(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'deserializeBinary' is not implemented for SerializationSparse");
}

void SerializationSparse::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextEscaped(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextEscaped(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'deserializeTextEscaped' is not implemented for SerializationSparse");
}

void SerializationSparse::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextQuoted(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextQuoted(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'deserializeTextQuoted' is not implemented for SerializationSparse");
}

void SerializationSparse::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextCSV(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextCSV(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'deserializeTextCSV' is not implemented for SerializationSparse");
}

void SerializationSparse::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeText(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeWholeText(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'deserializeWholeText' is not implemented for SerializationSparse");
}

void SerializationSparse::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextJSON(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextJSON(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'deserializeTextJSON' is not implemented for SerializationSparse");
}

void SerializationSparse::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextXML(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparseNullMap::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::SparseOffsets);
    settings.path.back().data = data;

    callback(settings.path);
    settings.path.pop_back();
}

void SerializationSparseNullMap::assertSettings(const SerializeBinaryBulkSettings & settings)
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SerializationSparseNullMap does not support serialization with position independent encoding");
}

void SerializationSparseNullMap::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    Base::serializeBinaryBulkStatePrefix(column, settings, state);
}

void SerializationSparseNullMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    Base::serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationSparseNullMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    Base::serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationSparseNullMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & /*settings*/,
    DeserializeBinaryBulkStatePtr & state) const
{
    /// There is no nested state in SerializationNumber
    state = std::make_shared<DeserializeStateSparse>();
}

void SerializationSparseNullMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * state_sparse = checkAndGetState<DeserializeStateSparse>(state);

    if (auto cached_column = getFromSubstreamsCache(cache, settings.path))
    {
        column = cached_column;
        return;
    }

    if (!settings.continuous_reading)
        state_sparse->reset();

    auto mutable_column = column->assumeMutable();
    auto & null_map_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();

    PaddedPODArray<UInt64> data_offsets;

    /// Read offsets of sparse columns.
    size_t read_rows = 0;
    settings.path.push_back(Substream::SparseOffsets);
    if (auto * stream = settings.getter(settings.path))
        read_rows = deserializeOffsets(data_offsets, *stream, null_map_data.size(), limit, *state_sparse);

    settings.path.pop_back();

    if (read_rows)
    {
        /// Restore null map from offsets.
        null_map_data.resize_fill(null_map_data.size() + read_rows, static_cast<UInt8>(1));
        for (UInt64 offset : data_offsets)
            null_map_data[offset] = 0;
    }

    column = std::move(mutable_column);
    addToSubstreamsCache(cache, settings.path, column);
}

}
