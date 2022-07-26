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
        DataPartWidePtr data_part_,
        NamesAndTypesList columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, size_t current_task_last_mark,
                    bool continue_reading, size_t max_rows_to_read, Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

    using FileStreams = std::map<std::string, std::unique_ptr<MergeTreeReaderStream>>;

private:
    FileStreams streams;

    String getNameForSubstreamCache(const NameAndTypePair & column) const;

    void addStreams(const NameAndTypePair & name_and_type,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    void readData(
        const NameAndTypePair & name_and_type, ColumnPtr & column,
        size_t from_mark, bool continue_reading, size_t current_task_last_mark, size_t max_rows_to_read,
        ISerialization::SubstreamsCache & cache, bool was_prefetched);

    /// Make next readData more simple by calling 'prefetch' of all related ReadBuffers (column streams).
    void prefetch(
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        bool continue_reading,
        size_t current_task_last_mark,
        ISerialization::SubstreamsCache & cache,
        std::unordered_set<std::string> & prefetched_streams); /// if stream was already prefetched do nothing

    void deserializePrefix(
        const SerializationPtr & serialization,
        const NameAndTypePair & name_and_type,
        size_t current_task_last_mark,
        ISerialization::SubstreamsCache & cache);
};

}
