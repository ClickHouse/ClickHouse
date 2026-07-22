#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationStringSize.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

SerializationStringSize::SerializationStringSize(MergeTreeStringSerializationVersion version_)
    : version(version_)
    , serialization_string(SerializationString::create(version))
{
}


UInt128 SerializationStringSize::getHash(MergeTreeStringSerializationVersion version_)
{
    SipHash hash;
    hash.update("StringSize");
    hash.update(static_cast<int>(version_));
    return hash.get128();
}

SerializationPtr SerializationStringSize::create(MergeTreeStringSerializationVersion version_)
{
    return ISerialization::pooled(getHash(version_), [=] { return new SerializationStringSize(version_); });
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
    IColumn & column,
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
    IColumn & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);
    auto * string_state = checkAndGetState<DeserializeBinaryBulkStateStringWithoutSizeStream>(state);

    if (string_state->need_string_data)
        deserializeWithStringData(column, rows_offset, limit, settings, cache);
    else
        deserializeWithoutStringData(column, rows_offset, limit, settings, cache);

    settings.path.pop_back();
}

void SerializationStringSize::deserializeWithStringData(
    IColumn & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    SubstreamsCache * cache) const
{
    size_t num_read_rows = 0;
    ColumnPtr string_column;

    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        std::tie(string_column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_string_column = ColumnString::create();
        double avg_value_size_hint
            = settings.get_avg_value_size_hint_callback ? settings.get_avg_value_size_hint_callback(settings.path) : 0.0;

        serialization_string->deserializeBinaryBulk(*mutable_string_column, *stream, rows_offset, limit, avg_value_size_hint);

        num_read_rows = mutable_string_column->size();
        string_column = std::move(mutable_string_column);
        /// Put the full String column into the cache so that a sibling read of the actual String column reuses it.
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, string_column, num_read_rows);

        if (settings.update_avg_value_size_hint_callback)
            settings.update_avg_value_size_hint_callback(settings.path, *string_column);
    }
    else
    {
        return;
    }

    auto & sizes_data = assert_cast<ColumnUInt64 &>(column).getData();
    sizes_data.reserve(sizes_data.size() + num_read_rows);

    const auto & offsets = assert_cast<const ColumnString &>(*string_column).getOffsets();
    size_t prev_size = offsets.size() - num_read_rows;
    for (size_t i = prev_size; i != offsets.size(); ++i)
        sizes_data.push_back(offsets[i] - offsets[i - 1]);
}

void SerializationStringSize::deserializeWithoutStringData(
    IColumn & column, size_t rows_offset, size_t limit, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache) const
{
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        return;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        for (size_t i = 0; unlikely(i < rows_offset); ++i)
        {
            UInt64 size = 0;
            readVarUInt(size, *stream);
            stream->ignore(size);
        }

        size_t prev_size = column.size();
        auto & mutable_column_data = typeid_cast<ColumnVector<UInt64> &>(column).getData();
        mutable_column_data.resize(prev_size + limit);

        size_t num_read_rows = 0;
        for (; likely(num_read_rows < limit); ++num_read_rows)
        {
            if (unlikely(stream->eof()))
                break;
            UInt64 size = 0;
            readVarUInt(size, *stream);
            stream->ignore(size);
            mutable_column_data[prev_size + num_read_rows] = size;
        }
        mutable_column_data.resize(prev_size + num_read_rows);

        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), num_read_rows);
    }
}

void SerializationStringSize::deserializeBinaryBulkWithSizeStream(
    IColumn & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::StringSizes);

    size_t prev_size = column.size();
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        /// Do nothing, data was inserted from cache.
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        /// Deserialize rows_offset + limit rows, we will apply rows_offset later.
        deserializeBinaryBulk(column, *stream, 0, rows_offset + limit, 0);

        if (cache)
        {
            size_t num_read_rows = column.size() - prev_size;
            /// rows_offset is applied in place below, so cache an unmodified cut() copy for other readers of this substream.
            if (rows_offset)
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.cut(prev_size, num_read_rows), num_read_rows);
            else
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), num_read_rows);
        }
    }

    /// Apply rows_offset if needed.
    if (rows_offset)
    {
        auto & data = assert_cast<ColumnUInt64 &>(column).getData();
        size_t actual_new_size = column.size() - rows_offset;
        for (size_t i = prev_size; i != actual_new_size; ++i)
            data[i] = data[i + rows_offset];
        data.resize(actual_new_size);
    }

    settings.path.pop_back();
}

size_t SerializationStringSize::allocatedBytes() const
{
    return sizeof(*this);
}

}
