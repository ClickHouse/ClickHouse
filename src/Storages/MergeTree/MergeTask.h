#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include "Storages/MergeTree/ColumnSizeEstimator.h"

#include <memory>
#include <list>
namespace DB
{


class BackgroundTask
{
public:
    virtual bool execute() = 0;
    virtual ~BackgroundTask() = default;
};


class MergeTask;

using MergeTaskPtr = std::shared_ptr<MergeTask>;

/**
 * States of MergeTask state machine.
 * Transitions are from up to down.
 * But for vertical merge there are horizontal part of the merge and vertical part.
 * For horizontal there is horizontal part only.
 */
enum class MergeTaskState
{
    NEED_PREPARE,
    NEED_EXECUTE_HORIZONTAL,
    NEED_EXECUTE_
    NEED_EXECUTE_VERTICAL,
    NEED_FINISH,
};

class MergeTask : public BackgroundTask
{
public:
    bool execute() override;

    void prepare();
private:
    class MergeImpl;
    class VerticalMergeImpl;
    class HorizontalMergeImpl;

    friend class MergeImpl;
    friend class VerticalMergeImpl;

    BlockInputStreamPtr createMergedStream();
    std::unique_ptr<MergeImpl> createMergeAlgorithmImplementation();
    bool executeHorizontalForBlock();

    MergeTaskState state;

    std::unique_ptr<MergeImpl> implementation;

    FutureMergedMutatedPartPtr future_part;
    StorageMetadataPtr metadata_snapshot;
    MergeList::EntryPtr merge_entry;
    TableLockHolderPtr holder;
    time_t time_of_merge;
    ContextPtr context;
    ReservationPtr space_reservation;
    bool deduplicate;
    NamesPtr deduplicate_by_columns;
    MergeTreeData::MergingParams merging_params;
    MergeTreeDataPartPtr parent_part;
    String prefix;

    /// From MergeTreeDataMergerMutator

    MergeTreeData & data;
    Poco::Logger * log;


    /// Previously stack located variables

    NamesAndTypesList gathering_columns;
    NamesAndTypesList merging_columns;
    Names gathering_column_names;
    Names merging_column_names;

    NamesAndTypesList storage_columns;
    Names all_column_names;

    String new_part_tmp_path;

    size_t sum_input_rows_upper_bound{0};

    bool need_remove_expired_values{false};
    bool force_ttl{false};

    DiskPtr tmp_disk{nullptr};
    DiskPtr disk{nullptr};

    String rows_sources_file_path;
    std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf{nullptr};
    std::unique_ptr<WriteBuffer> rows_sources_write_buf{nullptr};
    std::optional<ColumnSizeEstimator> column_sizes;

    SyncGuardPtr sync_guard{nullptr};

    MergeAlgorithm chosen_merge_algorithm;

    IMergedBlockOutputStreamPtr to;
    BlockInputStreamPtr merged_stream;

    bool blocks_are_granules_size{false};

    /// Variables that are needed for horizontal merge execution

    size_t rows_written{0};
    size_t initial_reservation{0};

    std::function<bool()> is_cancelled;
};


class MergeTask::MergeImpl
{
public:
    /**
     * This is used to access private fields of MergeTask class
     * Not to capture this fields by reference
    */
    MergeTask & task;
    explicit MergeImpl(MergeTask & task_) : task(task_) {}

    virtual void begin() = 0;

    virtual ~MergeImpl() = default;
};

class MergeTask::VerticalMergeImpl final : public MergeTask::MergeImpl
{
public:
    explicit VerticalMergeImpl(MergeTask & task_) : MergeImpl(task_) {}

    void begin() override;
};


class MergeTask::HorizontalMergeImpl final : public MergeTask::MergeImpl
{
public:
    explicit HorizontalMergeImpl(MergeTask & task_) : MergeImpl(task_) {}

    void begin() override;
};

/**
 * This is used for chaining merges of the main parts and projections.
*/
class MergeTaskChain : public BackgroundTask
{
public:
    bool execute() override;

    void add(MergeTaskPtr task);

private:
    std::list<MergeTaskPtr> tasks;
};



MergeTaskPtr createMergeTask()
{
    return std::make_shared<VerticalMergeTask>();
}


}
