#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>

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

class MergeTask : public BackgroundTask
{
public:
    bool execute() override;
private:
    virtual void chooseColumns() = 0;

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


class VerticalMergeTask final : public MergeTask
{
private:
    void chooseColumns() override;
};


class HorizontalMergeTask final : public MergeTask
{
private:
    void chooseColumns() override;
};


MergeTaskPtr createMergeTask()
{
    return std::make_shared<VerticalMergeTask>();
}


}
