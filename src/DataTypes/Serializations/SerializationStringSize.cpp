#include <DataTypes/Serializations/SerializationStringSize.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

SerializationStringSize::SerializationStringSize(MergeTreeStringSerializationVersion version_)
    : version(version_)
    , serialization_string(version)
{
}

void SerializationStringSize::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            settings.path.push_back(Substream::Regular);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            settings.path.push_back(Substream::StringSizes);
            break;
    }

    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationStringSize::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            deserializeBinaryBulkWithoutSizeStream(column, rows_offset, limit, settings, state, cache);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            deserializeBinaryBulkWithSizeStream(column, rows_offset, limit, settings, state, cache);
            break;
    }
}

void SerializationStringSize::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    if (version == MergeTreeStringSerializationVersion::SINGLE_STREAM)
    {
        settings.path.push_back(Substream::Regular);
        if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
        {
            state = cached_state;
        }
        else
        {
            auto string_state = std::make_shared<DeserializeBinaryBulkStateStringWithoutSizeStream>();

            /// If there is no state cache (e.g. StorageLog), we must always read the full string data. Without cached
            /// state, we cannot know in advance whether the string data will be needed later, and the string size has
            /// to be derived from the data itself.
            ///
            /// As a result, the subsequent deserialization relies on the substream cache to correctly share the string
            /// data across subcolumns. We do not support an optimization that deserializes only the size substream in
            /// this case, and therefore we must always populate the substream cache with the string data rather than
            /// the size-only substream.
            if (!cache)
                string_state->need_string_data = true;
            state = string_state;
            addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
        }
        settings.path.pop_back();
    }
}

void SerializationStringSize::deserializeBinaryBulkWithoutSizeStream(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);
    auto * string_state = checkAndGetState<DeserializeBinaryBulkStateStringWithoutSizeStream>(state);

    if (string_state->need_string_data)
        deserializeWithStringData(column, rows_offset, limit, settings, *string_state, cache);
    else
        deserializeWithoutStringData(column, rows_offset, limit, settings, cache);

    settings.path.pop_back();
}

void SerializationStringSize::deserializeWithStringData(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStateStringWithoutSizeStream & string_state,
    SubstreamsCache * cache) const
{
    size_t num_read_rows = 0;

    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        std::tie(string_state.column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        if (!string_state.column || column->empty())
            string_state.column = ColumnString::create();

        size_t prev_size = string_state.column->size();
        double avg_value_size_hint
            = settings.get_avg_value_size_hint_callback ? settings.get_avg_value_size_hint_callback(settings.path) : 0.0;

        serialization_string.deserializeBinaryBulk(*string_state.column->assumeMutable(), *stream, rows_offset, limit, avg_value_size_hint);

        num_read_rows = string_state.column->size() - prev_size;
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, string_state.column, num_read_rows);

        if (settings.update_avg_value_size_hint_callback)
            settings.update_avg_value_size_hint_callback(settings.path, *string_state.column);
    }
    else
    {
        return;
    }

    auto mutable_column = column->assumeMutable();
    auto & sizes_data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
    sizes_data.reserve(sizes_data.size() + num_read_rows);

    const auto & offsets = assert_cast<const ColumnString &>(*string_state.column).getOffsets();
    size_t prev_size = offsets.size() - num_read_rows;
    for (size_t i = prev_size; i != offsets.size(); ++i)
        sizes_data.push_back(offsets[i] - offsets[i - 1]);
}

void SerializationStringSize::deserializeWithoutStringData(
    ColumnPtr & column, size_t rows_offset, size_t limit, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache) const
{
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        return;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        for (size_t i = 0; unlikely(i < rows_offset); ++i)
        {
            UInt64 size;
            readVarUInt(size, *stream);
            stream->ignore(size);
        }

        size_t prev_size = column->size();
        auto mutable_column = column->assumeMutable();
        auto & mutable_column_data = typeid_cast<ColumnVector<UInt64> &>(*mutable_column).getData();
        mutable_column_data.resize(prev_size + limit);

        size_t num_read_rows = 0;
        for (; likely(num_read_rows < limit); ++num_read_rows)
        {
            if (unlikely(stream->eof()))
                break;
            UInt64 size;
            readVarUInt(size, *stream);
            stream->ignore(size);
            mutable_column_data[prev_size + num_read_rows] = size;
        }
        mutable_column_data.resize(prev_size + num_read_rows);
        column = std::move(mutable_column);

        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, num_read_rows);
    }
}

void SerializationStringSize::deserializeBinaryBulkWithSizeStream(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::StringSizes);

    size_t num_read_rows = 0;
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        auto cached_column = cached_column_with_num_read_rows->first;
        num_read_rows = cached_column_with_num_read_rows->second;

        /// Cached column contains data without applied rows_offset and can be used in other serializations (for example in SerializationString)
        /// so if rows_offset is not 0 we cannot use it as is because we will modify it here later by applying rows_offset.
        /// Instead we need to insert data from the current range from it.
        if (rows_offset)
            column->assumeMutable()->insertRangeFrom(*cached_column, cached_column->size() - num_read_rows, num_read_rows);
        else
            insertDataFromCachedColumn(settings, column, cached_column, num_read_rows, cache, true);
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_column = column->assumeMutable();
        size_t prev_size = mutable_column->size();
        /// Deserialize rows_offset + limit rows, we will apply rows_offset later.
        deserializeBinaryBulk(*mutable_column, *stream, 0, rows_offset + limit, 0);
        num_read_rows = mutable_column->size() - prev_size;

        if (cache)
        {
            ColumnPtr column_for_cache;
            /// If rows_offset != 0 we should keep data without applied offsets in the cache to be able
            /// to calculate offset for string data in SerializationString if the whole string is also read.
            /// As we will apply offsets to the current column we cannot put in the cache, so we use cut()
            /// method to create a separate column with all the data from current range.
            if (rows_offset)
                column_for_cache = mutable_column->cut(prev_size, num_read_rows);
            else
                column_for_cache = column;

            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column_for_cache, num_read_rows);
        }
    }

    /// Apply rows_offset if needed.
    if (rows_offset)
    {
        auto mutable_column = column->assumeMutable();
        auto & data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
        size_t prev_size = mutable_column->size() - num_read_rows;
        size_t actual_new_size = mutable_column->size() - rows_offset;
        for (size_t i = prev_size; i != actual_new_size; ++i)
            data[i] = data[i + rows_offset];
        data.resize(actual_new_size);
    }

    settings.path.pop_back();
}

}
