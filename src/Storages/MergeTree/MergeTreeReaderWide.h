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
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

    void prefetchBeginOfRange(Priority priority) override;

    using FileStreams = std::map<std::string, std::unique_ptr<MergeTreeReaderStream>>;

private:
    FileStreams streams;

    void prefetchForAllColumns(
        Priority priority,
        size_t num_columns,
        size_t from_mark,
        size_t current_task_last_mark,
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
        size_t current_task_last_mark,
        ISerialization::SubstreamsCache & cache);

    FileStreams::iterator addStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name);

    void readData(
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        ColumnPtr & column,
        size_t from_mark,
        bool continue_reading,
        size_t current_task_last_mark,
        size_t max_rows_to_read,
        size_t rows_offset,
        ISerialization::SubstreamsCache & cache,
        ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache);

    /// Make next readData more simple by calling 'prefetch' of all related ReadBuffers (column streams).
    void prefetchForColumn(
        Priority priority,
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        size_t from_mark,
        bool continue_reading,
        size_t current_task_last_mark,
        ISerialization::SubstreamsCache & cache);

    void deserializePrefix(
        const SerializationPtr & serialization,
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        size_t current_task_last_mark,
        DeserializeBinaryBulkStateMap & deserialize_state_map,
        ISerialization::SubstreamsCache & cache,
        ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache,
        ISerialization::StreamCallback prefixes_prefetch_callback);

    void deserializePrefixForAllColumns(size_t num_columns, size_t from_mark, size_t current_task_last_mark);
    void deserializePrefixForAllColumnsWithPrefetch(size_t num_columns, size_t from_mark, size_t current_task_last_mark, Priority priority);

    using StreamCallbackGetter = std::function<ISerialization::StreamCallback(const NameAndTypePair &)>;
    void deserializePrefixForAllColumnsImpl(size_t num_columns, size_t from_mark, size_t current_task_last_mark, StreamCallbackGetter prefixes_prefetch_callback_getter);

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;
    DeserializationPrefixesCache * deserialization_prefixes_cache;
    std::unordered_set<std::string> prefetched_streams;
    ssize_t prefetched_from_mark = -1;
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;
    bool read_without_marks = false;
};

}
