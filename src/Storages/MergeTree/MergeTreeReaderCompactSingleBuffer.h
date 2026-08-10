#pragma once
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

namespace DB
{

/// Reader for compact parts, that uses one buffer for
/// all column and doesn't support parallel prefetch of columns.
/// It's suitable for compact parts with small size of stripe.
class MergeTreeReaderCompactSingleBuffer : public MergeTreeReaderCompact
{
public:
    template <typename... Args>
    explicit MergeTreeReaderCompactSingleBuffer(Args &&... args)
        : MergeTreeReaderCompact{std::forward<Args>(args)...}
    {
        fillColumnPositions();
    }

    /// Returns the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark,
                    bool continue_reading, size_t max_rows_to_read,
                    size_t rows_offset, Columns & res_columns) override;

private:
    MergeTreeReaderStream & getStream(const NameAndTypePair &) override { return *stream; }

    void updatePlannedLastMark(size_t planned_last_mark) override
    {
        /// Keep the settings current for the lazily-created stream (`init`),
        /// and re-announce on the live one.
        settings.planned_last_mark = planned_last_mark;
        if (stream)
            stream->updatePlannedLastMark(planned_last_mark);
    }

    void updateRequestMap(std::vector<std::pair<size_t, size_t>> mark_ranges) override
    {
        settings.planned_mark_ranges = mark_ranges;
        if (stream)
            stream->updateRequestMap(std::move(mark_ranges));
    }
    void init();

    bool initialized = false;
    std::unique_ptr<MergeTreeReaderStream> stream;
};

}
