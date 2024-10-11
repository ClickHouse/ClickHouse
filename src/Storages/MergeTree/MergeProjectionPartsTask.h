#pragma once

#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class MergeProjectionPartsTask : public IExecutableTask
{
public:

    MergeProjectionPartsTask(
        String name_,
        MergeTreeData::MutableDataPartsVector && parts_,
        const ProjectionDescription & projection_,
        size_t & block_num_,
        ContextPtr context_,
        TableLockHolder * table_lock_holder_,
        MergeTreeDataMergerMutator * mutator_,
        MergeListEntry * merge_entry_,
        time_t time_of_merge_,
        MergeTreeData::MutableDataPartPtr new_data_part_,
        ReservationSharedPtr space_reservation_)
        : name(std::move(name_))
        , parts(std::move(parts_))
        , projection(projection_)
        , block_num(block_num_)
        , context(context_)
        , table_lock_holder(table_lock_holder_)
        , mutator(mutator_)
        , merge_entry(merge_entry_)
        , time_of_merge(time_of_merge_)
        , new_data_part(new_data_part_)
        , space_reservation(space_reservation_)
        , log(getLogger("MergeProjectionPartsTask"))
        {
            LOG_DEBUG(log, "Selected {} projection_parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
            level_parts[current_level] = std::move(parts);
        }

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override;

private:
    String name;
    MergeTreeData::MutableDataPartsVector parts;
    const ProjectionDescription & projection;
    size_t & block_num;

    ContextPtr context;
    TableLockHolder * table_lock_holder;
    MergeTreeDataMergerMutator * mutator;
    MergeListEntry * merge_entry;
    time_t time_of_merge;

    MergeTreeData::MutableDataPartPtr new_data_part;
    ReservationSharedPtr space_reservation;

    LoggerPtr log;

    std::map<size_t, MergeTreeData::MutableDataPartsVector> level_parts;
    size_t current_level = 0;
    size_t next_level = 1;

    /// TODO(nikitamikhaylov): make this constant a setting
    static constexpr size_t max_parts_to_merge_in_one_level = 10;
};

}
