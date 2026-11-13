#pragma once

#include <Common/Priority.h>
#include <Storages/MergeTree/MergeTreeReaderCompact.h>

namespace DB
{

/// Reader for compact parts, that uses one buffer for
/// each column and supports parallel prefetch of columns.
/// It's suitable for compact parts with large size of stripe.
class MergeTreeReaderCompactMultiBuffer : public MergeTreeReaderCompact
{
public:
    template <typename... Args>
    explicit MergeTreeReaderCompactMultiBuffer(Args &&... args)
        : MergeTreeReaderCompact{std::forward<Args>(args)...}
        , use_prefetch(data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
            ? settings.read_settings.remote_fs_settings.prefetch
            : settings.read_settings.local_fs_settings.prefetch)
    {
        fillColumnPositions();
        addStreams();
    }

    /// Prefetch begin of mark_ranges.
    void prefetchBeginOfRange(Priority priority) override;

    void updateReadRequestMap(MarkRangesPtr request_map) override;

    /// Returns the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(
        size_t from_mark,
        size_t current_range_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        MutableColumns & res_columns, Columns & shared_columns) override;

private:
    void addStreams();
    void seekToMarkAndDeserializePrefix(size_t from_mark, bool seek_to_mark);
    void adjustLastRightMark(size_t current_mark);

    MergeTreeReaderStream & getStream(const NameAndTypePair & column) override;

    const bool use_prefetch;
    FileStreams streams;
    FileStreams subcolumns_streams;

    /// For asynchronous reading from remote fs.
    /// Last right mark of stripe that we are
    /// currently reading.
    std::optional<size_t> last_right_mark;
};

}
