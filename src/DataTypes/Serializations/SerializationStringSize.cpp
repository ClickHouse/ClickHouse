#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationStringSize.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/StringGranuleDictionary.h>
#include <IO/VarInt.h>
#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

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
        case MergeTreeStringSerializationVersion::WITH_DICTIONARIES:
            /// The lengths are in three streams: the records of the granules with the codes, the lengths of the rows of
            /// plain granules, and the lengths of the dictionary values. The bytes are never read.
            settings.path.push_back(Substream::StringDictionaryIndexes);
            callback(settings.path);
            settings.path.back() = Substream::StringSizes;
            settings.path.back().data = data;
            callback(settings.path);
            settings.path.back() = Substream::StringDictionarySizes;
            callback(settings.path);
            settings.path.pop_back();
            return;
    }

    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationStringSize::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            deserializeBinaryBulkWithoutSizeStream(column, limit, settings, state, cache);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            deserializeBinaryBulkWithSizeStream(column, limit, settings, state, cache);
            break;
        case MergeTreeStringSerializationVersion::WITH_DICTIONARIES:
            deserializeBinaryBulkWithDictionaries(column, limit, settings, state, cache);
            break;
    }
}

void SerializationStringSize::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    if (version == MergeTreeStringSerializationVersion::WITH_DICTIONARIES)
    {
        state = getStringGranulesDeserializeState(settings, cache);
        /// Without a states cache nothing tells whether the column is read too, and the lengths are taken from it, as
        /// for `SINGLE_STREAM` below.
        if (!cache)
            checkAndGetState<DeserializeBinaryBulkStateStringWithDictionaries>(state)->need_string_data = true;
        return;
    }

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
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);
    auto * string_state = checkAndGetState<DeserializeBinaryBulkStateStringWithoutSizeStream>(state);

    if (string_state->need_string_data)
        deserializeWithStringData(column, limit, settings, cache);
    else
        deserializeWithoutStringData(column, limit, settings, cache);

    settings.path.pop_back();
}

void SerializationStringSize::deserializeWithStringData(
    IColumn & column,
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

        serialization_string->deserializeBinaryBulk(*mutable_string_column, *stream, limit, avg_value_size_hint);

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
    IColumn & column, size_t limit, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache) const
{
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        return;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
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
        deserializeBinaryBulk(column, *stream, limit, 0);

        if (cache)
        {
            size_t num_read_rows = column.size() - prev_size;
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), num_read_rows);
        }
    }

    settings.path.pop_back();
}

SerializationPtr SerializationStringSize::resolveForEmptySubstreams(const SubstreamIsEmptyCallback & substream_is_empty) const
{
    SubstreamPath dictionary_sizes;
    dictionary_sizes.push_back(Substream::StringDictionarySizes);
    if (version == MergeTreeStringSerializationVersion::WITH_DICTIONARIES && substream_is_empty(dictionary_sizes))
        return SerializationStringSize::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);
    return getPtr();
}

size_t SerializationStringSize::allocatedBytes() const
{
    return sizeof(*this);
}

namespace
{

/// Appends the lengths of rows `[start, start + length)` of `column`, a `ColumnString` or a `ColumnReplicated` over one.
void appendStringSizes(PaddedPODArray<UInt64> & sizes, const IColumn & column, size_t start, size_t length)
{
    sizes.reserve(sizes.size() + length);
    if (const auto * replicated = typeid_cast<const ColumnReplicated *>(&column))
    {
        const auto & offsets = assert_cast<const ColumnString &>(*replicated->getNestedColumn()).getOffsets();
        const auto & indexes = replicated->getIndexes();
        for (size_t row = start; row < start + length; ++row)
        {
            const size_t index = indexes.getIndexAt(row);
            sizes.push_back(offsets[index] - offsets[index - 1]);
        }
        return;
    }

    const auto & offsets = assert_cast<const ColumnString &>(column).getOffsets();
    for (size_t row = start; row < start + length; ++row)
        sizes.push_back(offsets[row] - offsets[row - 1]);
}

/// Appends `dictionary_sizes[code]` for the codes of rows `[start, start + length)`.
void appendDictionarySizes(
    PaddedPODArray<UInt64> & sizes, const PaddedPODArray<UInt64> & dictionary_sizes, const IColumn & codes, size_t start, size_t length)
{
    ColumnIndex::callForIndexesData(codes, [&](const auto & data)
    {
        const size_t prev_size = sizes.size();
        sizes.resize(prev_size + length);
        UInt64 * out = sizes.data() + prev_size;
        const auto * in = data.data() + start;
        for (size_t i = 0; i < length; ++i)
            out[i] = dictionary_sizes[in[i]];
    });
}

}

void SerializationStringSize::deserializeBinaryBulkWithDictionaries(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & granules_state = *checkAndGetState<DeserializeBinaryBulkStateStringWithDictionaries>(state);
    auto & sizes = assert_cast<ColumnUInt64 &>(column).getData();

    if (granules_state.need_string_data)
    {
        /// The column is read as well, from the same streams. Take the lengths from it, and read it here if this
        /// subcolumn comes first: the column then finds itself in the substreams cache.
        settings.path.push_back(Substream::Regular);
        auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path);
        settings.path.pop_back();

        ColumnPtr string_column;
        size_t num_read_rows = 0;
        if (cached_column_with_num_read_rows)
        {
            std::tie(string_column, num_read_rows) = *cached_column_with_num_read_rows;
        }
        else
        {
            auto read_column = ColumnReplicated::create(MutableColumnPtr(ColumnString::create()), MutableColumnPtr(ColumnUInt8::create()));
            serialization_string->deserializeBinaryBulkWithMultipleStreams(*read_column, limit, settings, state, cache);
            num_read_rows = read_column->size();
            string_column = std::move(read_column);
        }

        appendStringSizes(sizes, *string_column, string_column->size() - num_read_rows, num_read_rows);
        return;
    }

    /// Each stream is requested once per call, see `SerializationString::deserializeBinaryBulkWithDictionaries`.
    settings.path.push_back(Substream::StringDictionaryIndexes);
    ReadBuffer * indexes_stream = settings.getter(settings.path);
    settings.path.back() = Substream::StringSizes;
    ReadBuffer * sizes_stream = settings.getter(settings.path);
    settings.path.back() = Substream::StringDictionarySizes;
    ReadBuffer * dictionary_sizes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    /// The column is not in the part.
    if (!indexes_stream)
        return;

    if (!settings.continuous_reading)
        granules_state.rows_left_in_granule = 0;

    sizes.reserve(sizes.size() + limit);
    size_t rows_read = 0;
    while (rows_read < limit)
    {
        const auto & granule = granules_state.granule;
        if (granules_state.rows_left_in_granule == 0)
        {
            if (!granules_state.startGranule(*indexes_stream))
                break;

            if (granule.kind == StringGranuleHeader::Kind::Dictionary)
            {
                granules_state.dictionary_sizes.clear();
                SerializationNumber<UInt64>::deserializeBinaryBulk(granules_state.dictionary_sizes, *dictionary_sizes_stream, granule.dictionary_size);
                if (granules_state.dictionary_sizes.size() != granule.dictionary_size)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the dictionary lengths of a String granule");
            }
        }

        const size_t rows = std::min(limit - rows_read, granules_state.rows_left_in_granule);
        if (granule.kind == StringGranuleHeader::Kind::Plain)
        {
            const size_t prev_size = sizes.size();
            SerializationNumber<UInt64>::deserializeBinaryBulk(sizes, *sizes_stream, rows);
            if (sizes.size() - prev_size != rows)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the lengths of {} rows of a plain String granule", rows);
        }
        else
        {
            appendDictionarySizes(sizes, granules_state.dictionary_sizes, *granules_state.granule_codes, granules_state.firstUnreadRow(), rows);
        }

        granules_state.rows_left_in_granule -= rows;
        rows_read += rows;
    }
}

}
