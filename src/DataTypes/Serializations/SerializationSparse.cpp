#include <DataTypes/Serializations/SerializationSparse.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

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
    /// Column offsets from previous read.
    /// Used only in SerializationSparseNullMap.
    ColumnPtr column_offsets;
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
size_t deserializeOffsets(
    IColumn::Offsets & offsets,
    ReadBuffer & istr,
    size_t start,
    size_t offset,
    size_t limit,
    size_t & skipped_values_rows,
    DeserializeStateSparse & state)
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
        + static_cast<size_t>(static_cast<double>(limit) * (1.0 - ColumnSparse::DEFAULT_RATIO_FOR_SPARSE_SERIALIZATION)));

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

size_t readOrGetCachedSparseOffsets(
    ColumnPtr & offsets_column,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::SubstreamsCache * cache,
    DeserializeStateSparse & state_sparse,
    size_t prev_size,
    size_t rows_offset,
    size_t limit,
    size_t & read_rows,
    size_t & skipped_values_rows)
{
    settings.path.push_back(ISerialization::Substream::SparseOffsets);
    const auto * cached_element = ISerialization::getElementFromSubstreamsCache(cache, settings.path);

    size_t num_read_offsets = 0;
    if (cached_element)
    {
        /// Reuse cached offsets info
        const auto & cached_offsets_element = assert_cast<const SubstreamsCacheSparseOffsetsElement &>(*cached_element);
        num_read_offsets = cached_offsets_element.offsets->size() - cached_offsets_element.old_size;
        read_rows = cached_offsets_element.read_rows;
        skipped_values_rows = cached_offsets_element.skipped_values_rows;
        ISerialization::insertDataFromCachedColumn(settings, offsets_column, cached_offsets_element.offsets, num_read_offsets, cache);
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        if (!settings.continuous_reading)
            state_sparse.reset();

        auto & offsets_data = assert_cast<ColumnUInt64 &>(offsets_column->assumeMutableRef()).getData();
        size_t old_size = offsets_data.size();
        read_rows = deserializeOffsets(offsets_data, *stream, prev_size, rows_offset, limit, skipped_values_rows, state_sparse);

        ISerialization::addElementToSubstreamsCache(
            cache,
            settings.path,
            std::make_unique<SubstreamsCacheSparseOffsetsElement>(offsets_column, old_size, read_rows, skipped_values_rows));
        num_read_offsets = offsets_column->size() - old_size;
    }

    settings.path.pop_back();
    return num_read_offsets;
}

}

SerializationSparse::SerializationSparse(const SerializationPtr & nested_)
    : nested(nested_)
{
    if (const auto * nested_nullable = typeid_cast<const SerializationNullable *>(nested.get()))
    {
        nested = std::make_shared<SerializationNullable>(nested_nullable->getNested(), true /* use_default_null_map */);
        sparse_null_map = std::make_shared<SerializationSparseNullMap>();
    }
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

ColumnPtr SerializationSparse::NullMapSubcolumnCreator::create(const ColumnPtr & /* prev */) const
{
    auto null_map_col = ColumnUInt8::create();
    auto & null_map_data = null_map_col->getData();
    null_map_data.resize_fill(size, static_cast<UInt8>(1));
    const auto & offsets_data = assert_cast<const ColumnUInt64 &>(*offsets).getData();
    for (UInt64 offset : offsets_data)
        null_map_data[offset] = 0;
    return null_map_col;
}

void SerializationSparse::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
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

    if (sparse_null_map)
    {
        const auto * type_nullable = data.type ? &assert_cast<const DataTypeNullable &>(*data.type) : nullptr;
        settings.path.back() = Substream::SparseNullMap;
        settings.path.back().creator = std::make_shared<NullMapSubcolumnCreator>(offsets_data.column, column_size);
        auto null_map_data = SubstreamData(sparse_null_map)
                                 .withType(type_nullable ? std::make_shared<DataTypeUInt8>() : nullptr)
                                 .withColumn(nullptr) /// subcolumn will be created in NullMapSubcolumnCreator based on offsets
                                 .withSerializationInfo(data.serialization_info);
        settings.path.back().data = null_map_data;
        callback(settings.path);
    }

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
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column))
        nested->serializeBinaryBulkStatePrefix(column_sparse->getValuesColumn(), settings, state);
    else
        nested->serializeBinaryBulkStatePrefix(column, settings, state);

    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
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

void SerializationSparse::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    /// Use Substream::SparseOffsets as the cache key for SparseState,
    /// because this state is also shared by the SparseNullMap substream.
    settings.path.push_back(Substream::SparseOffsets);
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = cached_state;
    }
    else
    {
        state = std::make_shared<DeserializeStateSparse>();
        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }

    settings.path.back() = Substream::SparseElements;
    nested->deserializeBinaryBulkStatePrefix(settings, checkAndGetState<DeserializeStateSparse>(state)->nested, cache);
    settings.path.pop_back();
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

    /// Reading SparseOffsets first.
    auto mutable_column = column->assumeMutable();
    auto & column_sparse = assert_cast<ColumnSparse &>(*mutable_column);
    auto & offsets_column = column_sparse.getOffsetsPtr();
    size_t prev_size = column_sparse.size();
    size_t read_rows = 0;
    size_t skipped_values_rows = 0;
    size_t num_read_offsets
        = readOrGetCachedSparseOffsets(offsets_column, settings, cache, *state_sparse, prev_size, rows_offset, limit, read_rows, skipped_values_rows);

    /// Reading SparseValues and constructing ColumnSparse.
    auto & values_column = column_sparse.getValuesPtr();

    settings.path.push_back(Substream::SparseElements);
    /// We cannot use column from substream cache during deserialization of sparse values column, because
    /// sparse values column must always contain default value at the first row that is added during ColumnSparse
    /// creation. Using column from substream cache will lead to loss of this value and unexpected column size.
    /// So, we should set insert_only_rows_in_current_range_from_substreams_cache flag to true
    /// to insert only rows in current range from substream cache instead of using the whole cached column if any.
    auto values_settings = settings;
    values_settings.insert_only_rows_in_current_range_from_substreams_cache = true;
    nested->deserializeBinaryBulkWithMultipleStreams(
        values_column, skipped_values_rows, num_read_offsets, values_settings, state_sparse->nested, cache);
    settings.path.pop_back();

    if (offsets_column->size() + 1 != values_column->size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inconsistent sizes of values and offsets in SerializationSparse. Offsets size: {}, values size: {}",
            offsets_column->size(),
            values_column->size());
    }

    /// 'insertManyDefaults' just increases size of column.
    column_sparse.insertManyDefaults(read_rows);
    column = std::move(mutable_column);
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

void SerializationSparseNullMap::assertSettings(const SerializeBinaryBulkSettings & settings)
{
    if (settings.position_independent_encoding)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "SerializationSparseNullMap does not support serialization with position independent encoding");
    }
}

void SerializationSparseNullMap::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    Base::serializeBinaryBulkStatePrefix(column, settings, state);
}

void SerializationSparseNullMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    Base::serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationSparseNullMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    assertSettings(settings);
    Base::serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationSparseNullMap::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    settings.path.push_back(Substream::SparseNullMap);
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationSparseNullMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    /// Use Substream::SparseOffsets as the cache key for SparseState,
    /// because this state is also shared by the Sparse stream.
    settings.path.push_back(Substream::SparseOffsets);
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = cached_state;
    }
    else
    {
        state = std::make_shared<DeserializeStateSparse>();
        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }
}

void SerializationSparseNullMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * state_sparse = checkAndGetState<DeserializeStateSparse>(state);

    /// Reading SparseOffsets first.

    /// Initialize offsets column in state if needed.
    if (!state_sparse->column_offsets || column->empty())
        state_sparse->column_offsets = ColumnUInt64::create();

    size_t prev_size = column->size();
    size_t read_rows = 0;
    size_t skipped_values_rows = 0;
    size_t num_read_offsets
        = readOrGetCachedSparseOffsets(state_sparse->column_offsets, settings, cache, *state_sparse, prev_size, rows_offset, limit, read_rows, skipped_values_rows);

    /// Converting SparseOffsets to NullMap.

    if (read_rows)
    {
        auto mutable_column = column->assumeMutable();
        auto & null_map_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        const auto & offsets_data = assert_cast<const ColumnUInt64 &>(*state_sparse->column_offsets).getData();

        /// Restore null map from offsets.
        null_map_data.resize_fill(null_map_data.size() + read_rows, static_cast<UInt8>(1));
        size_t total_offsets = offsets_data.size();
        for (size_t i = total_offsets - num_read_offsets; i < total_offsets; ++i)
            null_map_data[offsets_data[i]] = 0;

        column = std::move(mutable_column);
    }
}

}
