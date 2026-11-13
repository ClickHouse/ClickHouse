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
    /// Seek the streams of the columns `columns_mask` selects (all of them if it is null) to `from_mark`
    /// (only those that need it, unless `seek_to_mark`) and deserialize their prefixes.
    void seekToMarkAndDeserializePrefix(size_t from_mark, bool seek_to_mark, const std::vector<bool> * columns_mask);
    void adjustLastRightMark(size_t current_mark);

    MergeTreeReaderStream & getStream(const NameAndTypePair & column) override;

    /// Read the granule `mark` of the columns `columns_mask` selects, see `MergeTreeReaderCompact::readGranules`.
    /// The columns cache serves some granules of some columns, so the other streams are not where the previous
    /// granule left them, and every selected stream is sought to the mark.
    void readGranule(size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask) override;

    /// Read the granule `mark`. The streams continue from where the previous granule left them, unless
    /// `seek_to_mark` or the granule begins a new stripe.
    void readGranuleImpl(size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask, bool seek_to_mark);

    const bool use_prefetch;
    FileStreams streams;
    FileStreams subcolumns_streams;

    /// For asynchronous reading from remote fs.
    /// Last right mark of stripe that we are
    /// currently reading.
    std::optional<size_t> last_right_mark;
};

}
