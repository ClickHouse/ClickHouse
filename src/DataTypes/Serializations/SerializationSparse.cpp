#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnSparse.h>
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

void serializeOffsets(const IColumn::Offsets & offsets, WriteBuffer & ostr, size_t start, size_t end)
{
    size_t size = offsets.size();
    for (size_t i = 0; i < size; ++i)
    {
        size_t group_size = offsets[i] - start;
        writeVarUInt(group_size, ostr);
        start += group_size + 1;
    }

    size_t group_size = start < end ? end - start : 0;
    group_size |= END_OF_GRANULE_FLAG;
    writeVarUInt(group_size, ostr);
}


/// Returns number of read rows.
/// @start is the size of column before reading offsets.
size_t deserializeOffsets(IColumn::Offsets & offsets,
    ReadBuffer & istr, size_t start, size_t limit, DeserializeStateSparse & state)
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

SerializationSparse::SerializationSparse(const SerializationPtr & nested_)
    : nested(nested_)
{
}

SerializationPtr SerializationSparse::SubcolumnCreator::create(const SerializationPtr & prev) const
{
    return std::make_shared<SerializationSparse>(prev);
}

ColumnPtr SerializationSparse::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnSparse::create(prev, offsets, size);
}

void SerializationSparse::enumerateStreams(
    SubstreamPath & path,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * column_sparse = data.column ? &assert_cast<const ColumnSparse &>(*data.column) : nullptr;

    size_t column_size = column_sparse ? column_sparse->size() : 0;

    path.push_back(Substream::SparseOffsets);
    path.back().data =
    {
        std::make_shared<SerializationNumber<UInt64>>(),
        data.type ? std::make_shared<DataTypeUInt64>() : nullptr,
        column_sparse ? column_sparse->getOffsetsPtr() : nullptr,
        data.serialization_info,
    };

    callback(path);

    path.back() = Substream::SparseElements;
    path.back().creator = std::make_shared<SubcolumnCreator>(path.back().data.column, column_size);
    path.back().data = data;

    SubstreamData next_data =
    {
        nested,
        data.type,
        column_sparse ? column_sparse->getValuesPtr() : nullptr,
        data.serialization_info,
    };

    nested->enumerateStreams(path, callback, next_data);
    path.pop_back();
}

void SerializationSparse::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    nested->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkWithMultipleStreams(
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
        serializeOffsets(offsets_data, *stream, offset, end);
    }

    if (!offsets_data.empty())
    {
        settings.path.back() = Substream::SparseElements;
        if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column))
        {
            const auto & values = column_sparse->getValuesColumn();
            size_t begin = column_sparse->getValueIndex(offsets_data[0]);
            size_t end = column_sparse->getValueIndex(offsets_data.back());
            nested->serializeBinaryBulkWithMultipleStreams(values, begin, end - begin + 1, settings, state);
        }
        else
        {
            auto values = column.index(*offsets_column, 0);
            nested->serializeBinaryBulkWithMultipleStreams(*values, 0, values->size(), settings, state);
        }
    }

    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    auto state_sparse = std::make_shared<DeserializeStateSparse>();

    settings.path.push_back(Substream::SparseElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state_sparse->nested);
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

    auto & values_column = column_sparse.getValuesPtr();
    size_t values_limit = offsets_data.size() - old_size;

    settings.path.back() = Substream::SparseElements;
    /// Do not use substream cache while reading values column, because ColumnSparse can be cached only in a whole.
    nested->deserializeBinaryBulkWithMultipleStreams(values_column, values_limit, settings, state_sparse->nested, nullptr);
    settings.path.pop_back();

    if (offsets_data.size() + 1 != values_column->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent sizes of values and offsets in SerializationSparse."
        " Offsets size: {}, values size: {}", offsets_data.size(), values_column->size());

    /// 'insertManyDefaults' just increases size of column.
    column_sparse.insertManyDefaults(read_rows);
    column = std::move(mutable_column);
    addToSubstreamsCache(cache, settings.path, column);
}

/// All methods below just wrap nested serialization.

void SerializationSparse::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    nested->serializeBinary(field, ostr);
}

void SerializationSparse::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    nested->deserializeBinary(field, istr);
}

void SerializationSparse::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeBinary(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr);
}

void SerializationSparse::deserializeBinary(IColumn &, ReadBuffer &) const
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

}
