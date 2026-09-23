#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace DB
{

class MergeTreeDataPartWide;
using DataPartWidePtr = std::shared_ptr<const MergeTreeDataPartWide>;

/// Reader for Wide parts.
class MergeTreeReaderWide : public IMergeTreeReader
{
public:
    MergeTreeReaderWide(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeSettingsPtr & storage_settings_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        DeserializationPrefixesCache * deserialization_prefixes_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(
        size_t from_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        MutableColumns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

    void prefetchBeginOfRange(Priority priority) override;

    /// Return map (column to read) -> (list of all streams required to read this column).
    std::unordered_map<String, std::vector<String>> getAllColumnsSubstreams();

private:
    /// Prefixes or data of a single column can be deserialized in parallel from different streams, so
    /// these containers are mutated concurrently and all of the reader's synchronization lives here.
    /// Only the containers are guarded: `MergeTreeReaderStream` is not thread safe and is used with the
    /// mutex released, which is safe because parallel tasks own disjoint stream names, so a stream and
    /// any `ReadBuffer` taken from it stay valid until the owning task calls `release`. The mutex is
    /// never held across stream construction or marks loading, both of which block.
    class FileStreams
    {
    public:
        using StreamFactory = std::function<std::unique_ptr<MergeTreeReaderStream>()>;

        /// `factory` runs only when the stream is missing, and with the mutex released: creating a
        /// stream schedules its marks load, which blocks when the marks pool queue is full.
        MergeTreeReaderStream * getOrCreate(const String & stream_name, const StreamFactory & factory);
        MergeTreeReaderStream * find(const String & stream_name) const;
        void release(const String & stream_name);

        bool isPrefetched(const String & stream_name) const;
        void markPrefetched(const String & stream_name);
        void unmarkPrefetched(const String & stream_name);
        void clearPrefetched();

    private:
        mutable std::mutex mutex;
        std::map<String, std::unique_ptr<MergeTreeReaderStream>> streams;
        std::unordered_set<String> prefetched;
    };

    FileStreams streams;

    void prefetchForAllColumns(
        Priority priority,
        size_t num_columns,
        size_t from_mark,
        bool continue_reading,
        bool deserialize_prefixes);

    void addStreams(
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization);

    ReadBuffer * getStream(
        bool seek_to_start,
        const ISerialization::SubstreamPath & substream_path,
        const MergeTreeDataPartChecksums & checksums,
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        bool seek_to_mark,
        ISerialization::SubstreamsCache & cache);

    MergeTreeReaderStream * getOrAddStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name);

    void readData(
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        IColumn & column,
        size_t from_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        ISerialization::SubstreamsCache & cache,
        ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache);

    /// Make next readData more simple by calling 'prefetch' of all related ReadBuffers (column streams).
    void prefetchForColumn(
        Priority priority,
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        size_t from_mark,
        bool continue_reading,
        ISerialization::SubstreamsCache & cache);

    void deserializePrefix(
        const SerializationPtr & serialization,
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        DeserializeBinaryBulkStateMap & deserialize_state_map,
        ISerialization::SubstreamsCache & cache,
        ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache,
        ISerialization::StreamCallback prefixes_prefetch_callback);

    void deserializePrefixForAllColumns(size_t num_columns, size_t from_mark);
    void deserializePrefixForAllColumnsWithPrefetch(size_t num_columns, size_t from_mark, Priority priority);

    using StreamCallbackGetter = std::function<ISerialization::StreamCallback(const NameAndTypePair &)>;
    void deserializePrefixForAllColumnsImpl(size_t num_columns, size_t from_mark, StreamCallbackGetter prefixes_prefetch_callback_getter);

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;
    DeserializationPrefixesCache * deserialization_prefixes_cache;
    ssize_t prefetched_from_mark = -1;
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;
    bool read_without_marks = false;
};

}
