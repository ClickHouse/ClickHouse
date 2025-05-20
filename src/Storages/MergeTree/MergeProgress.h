#pragma once

#include <base/types.h>
#include <Common/ProfileEvents.h>
#include <IO/Progress.h>
#include <Storages/MergeTree/MergeList.h>


namespace ProfileEvents
{
    extern const Event MergedUncompressedBytes;
    extern const Event MergedRows;
    extern const Event MutatedRows;
    extern const Event MutatedUncompressedBytes;
}

namespace DB
{

/** Progress callback.
  * What it should update:
  * - approximate progress
  * - amount of read rows
  * - various metrics
  * - time elapsed for current merge.
  */

/// Auxiliary struct that for each merge stage stores its current progress.
/// A stage is: the horizontal stage + a stage for each gathered column (if we are doing a
/// Vertical merge) or a mutation of a single part. During a single stage all rows are read.
struct MergeStageProgress
{
    explicit MergeStageProgress(Float64 weight_)
        : is_first(true) , weight(weight_) {}

    MergeStageProgress(Float64 initial_progress_, Float64 weight_)
        : initial_progress(initial_progress_), is_first(false), weight(weight_) {}

    Float64 initial_progress = 0.0;
    bool is_first;
    Float64 weight;

    UInt64 total_rows = 0;
    UInt64 rows_read = 0;
};

class MergeProgressCallback
{
public:
    MergeProgressCallback(
        MergeListElement * merge_list_element_ptr_, UInt64 & watch_prev_elapsed_, MergeStageProgress & stage_)
        : merge_list_element_ptr(merge_list_element_ptr_)
        , watch_prev_elapsed(watch_prev_elapsed_)
        , stage(stage_)
    {
        updateWatch();
    }

    MergeListElement * merge_list_element_ptr;
    UInt64 & watch_prev_elapsed;
    MergeStageProgress & stage;

    void updateWatch()
    {
        UInt64 watch_curr_elapsed = merge_list_element_ptr->watch.elapsed();
        watch_prev_elapsed = watch_curr_elapsed;
    }

    void operator()(const Progress & value)
    {
        if (merge_list_element_ptr->is_mutation)
            updateProfileEvents(value, ProfileEvents::MutatedRows, ProfileEvents::MutatedUncompressedBytes);
        else
            updateProfileEvents(value, ProfileEvents::MergedRows, ProfileEvents::MergedUncompressedBytes);


        updateWatch();

        merge_list_element_ptr->bytes_read_uncompressed += value.read_bytes;
        if (stage.is_first)
            merge_list_element_ptr->rows_read += value.read_rows;

        stage.total_rows += value.total_rows_to_read;
        stage.rows_read += value.read_rows;
        if (stage.total_rows > 0)
        {
            merge_list_element_ptr->progress.store(
                stage.initial_progress + stage.weight * stage.rows_read / stage.total_rows,
                std::memory_order_relaxed);
        }
    }

private:
    void updateProfileEvents(const Progress & value, ProfileEvents::Event rows_event, ProfileEvents::Event bytes_event) const
    {
        ProfileEvents::increment(bytes_event, value.read_bytes);
        if (stage.is_first)
            ProfileEvents::increment(rows_event, value.read_rows);
    }
};

}
