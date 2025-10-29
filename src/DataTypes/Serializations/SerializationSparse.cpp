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

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeStateSparse>(*this);
        new_state->nested = nested ? nested->clone() : nullptr;
        return new_state;
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
    ReadBuffer & istr, size_t start, size_t offset, size_t limit, size_t & skipped_values_rows, DeserializeStateSparse & state)
{
    skipped_values_rows = 0;
    size_t max_rows_to_read = offset + limit;

    if (max_rows_to_read && state.num_trailing_defaults >= max_rows_to_read)
    {
        state.num_trailing_defaults -= max_rows_to_read;
        return limit;
    }

    /// Just try to guess number of offsets.
    offsets.reserve(offsets.size()
        + static_cast<size_t>(limit * (1.0 - ColumnSparse::DEFAULT_RATIO_FOR_SPARSE_SERIALIZATION)));

    bool first = true;
    size_t total_rows = state.num_trailing_defaults;
    size_t tmp_offset = offset;
    if (state.has_value_after_defaults)
    {
        if (state.num_trailing_defaults >= tmp_offset)
        {
            offsets.push_back(start + state.num_trailing_defaults - tmp_offset);
            tmp_offset = 0;
            first = false;
        }
        else
        {
            ++skipped_values_rows;
            tmp_offset -= state.num_trailing_defaults + 1;
        }

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

        if (max_rows_to_read && next_total_rows >= max_rows_to_read)
        {
            /// If it was not last group in granule,
            /// we have to add current non-default value at further reads.
            state.num_trailing_defaults = next_total_rows - max_rows_to_read;
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

            if (group_size >= tmp_offset)
            {
                offsets.push_back(start_of_group + group_size - tmp_offset);
                tmp_offset = 0;
                first = false;
            }
            else
            {
                ++skipped_values_rows;
                tmp_offset -= group_size + 1;
            }

            state.num_trailing_defaults = 0;
            state.has_value_after_defaults = false;
            ++next_total_rows;
        }

        total_rows = next_total_rows;
    }

    return total_rows > offset ? total_rows - offset : 0;
}

}

SerializationSparse::SerializationSparse(const SerializationPtr & nested_)
    : nested(nested_)
{
}

ISerialization::KindStack SerializationSparse::getKindStack() const
{
    auto kind_stack = nested->getKindStack();
    kind_stack.push_back(Kind::SPARSE);
    return kind_stack;
}

SerializationPtr SerializationSparse::SubcolumnCreator::create(const SerializationPtr & prev, const DataTypePtr &) const
{
    return std::make_shared<SerializationSparse>(prev);
}

ColumnPtr SerializationSparse::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnSparse::create(prev, offsets, size);
}

void SerializationSparse::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * column_sparse = data.column ? typeid_cast<const ColumnSparse *>(data.column.get()) : nullptr;
    size_t column_size = column_sparse ? column_sparse->size() : 0;

    settings.path.push_back(Substream::SparseOffsets);
    auto offsets_data = SubstreamData(std::make_shared<SerializationNumber<UInt64>>())
        .withType(data.type ? std::make_shared<DataTypeUInt64>() : nullptr)
        .withColumn(column_sparse ? column_sparse->getOffsetsPtr() : nullptr)
        .withSerializationInfo(data.serialization_info);

    settings.path.back().data = offsets_data;
    callback(settings.path);

    settings.path.back() = Substream::SparseElements;
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(offsets_data.column, column_size);
    settings.path.back().data = data;

    auto next_data = SubstreamData(nested)
        .withType(data.type)
        .withColumn(column_sparse ? column_sparse->getValuesPtr() : data.column)
        .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column))
        nested->serializeBinaryBulkStatePrefix(column_sparse->getValuesColumn(), settings, state);
    else
        nested->serializeBinaryBulkStatePrefix(column, settings, state);

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

    settings.path.back() = Substream::SparseElements;
    if (!offsets_data.empty())
    {
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
    else
    {
        auto empty_column = column.cloneEmpty()->convertToFullColumnIfSparse();
        nested->serializeBinaryBulkWithMultipleStreams(*empty_column, 0, 0, settings, state);
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
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto state_sparse = std::make_shared<DeserializeStateSparse>();

    settings.path.push_back(Substream::SparseElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state_sparse->nested, cache);
    settings.path.pop_back();

    state = std::move(state_sparse);
}

void SerializationSparse::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * state_sparse = checkAndGetState<DeserializeStateSparse>(state);

    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        return;

    if (!settings.continuous_reading)
        state_sparse->reset();

    size_t prev_size = column->size();
    auto mutable_column = column->assumeMutable();
    auto & column_sparse = assert_cast<ColumnSparse &>(*mutable_column);

    size_t old_size = 0;
    size_t read_rows = 0;
    size_t skipped_values_rows = 0;
    settings.path.push_back(Substream::SparseOffsets);

    const auto * cached_element = getElementFromSubstreamsCache(cache, settings.path);
    if (cached_element)
    {
        const auto & cached_offsets_element = assert_cast<const SubstreamsCacheSparseOffsetsElement &>(*cached_element);
        column_sparse.getOffsetsPtr() = cached_offsets_element.offsets;
        old_size = cached_offsets_element.old_size;
        read_rows = cached_offsets_element.read_rows;
        skipped_values_rows = cached_offsets_element.skipped_values_rows;
    }
    else
    {
        if (auto * stream = settings.getter(settings.path))
        {
            auto & offsets_data = column_sparse.getOffsetsData();
            old_size = offsets_data.size();
            read_rows = deserializeOffsets(
                offsets_data, *stream, column_sparse.size(), rows_offset, limit, skipped_values_rows, *state_sparse);

            addElementToSubstreamsCache(
                cache,
                settings.path,
                std::make_unique<SubstreamsCacheSparseOffsetsElement>(
                    column_sparse.getOffsetsPtr(), old_size, read_rows, skipped_values_rows));
        }
    }

    auto & offsets_data = column_sparse.getOffsetsData();
    auto & values_column = column_sparse.getValuesPtr();
    size_t values_limit = offsets_data.size() - old_size;

    settings.path.back() = Substream::SparseElements;
    nested->deserializeBinaryBulkWithMultipleStreams(values_column, skipped_values_rows, values_limit, settings, state_sparse->nested, cache);
    settings.path.pop_back();

    if (offsets_data.size() + 1 != values_column->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent sizes of values and offsets in SerializationSparse."
        " Offsets size: {}, values size: {}", offsets_data.size(), values_column->size());

    /// 'insertManyDefaults' just increases size of column.
    column_sparse.insertManyDefaults(read_rows);
    column = std::move(mutable_column);
    addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
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

template <typename Reader>
void SerializationSparse::deserialize(IColumn & column, Reader && reader) const
{
    auto & column_sparse = assert_cast<ColumnSparse &>(column);
    auto & values = column_sparse.getValuesColumn();
    size_t old_size = column_sparse.size();

    /// It just increments the size of column.
    column_sparse.insertDefault();
    reader(column_sparse.getValuesColumn());

    if (values.isDefaultAt(values.size() - 1))
        values.popBack(1);
    else
        column_sparse.getOffsetsData().push_back(old_size);
}

void SerializationSparse::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeBinary(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeBinary(nested_column, istr, settings);
    });
}

void SerializationSparse::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextEscaped(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextEscaped(nested_column, istr, settings);
    });
}

void SerializationSparse::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextQuoted(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextQuoted(nested_column, istr, settings);
    });
}

void SerializationSparse::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextCSV(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextCSV(nested_column, istr, settings);
    });
}

void SerializationSparse::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeText(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeWholeText(nested_column, istr, settings);
    });
}

void SerializationSparse::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextJSON(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

void SerializationSparse::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextJSON(nested_column, istr, settings);
    });
}

void SerializationSparse::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(column);
    nested->serializeTextXML(column_sparse.getValuesColumn(), column_sparse.getValueIndex(row_num), ostr, settings);
}

}
