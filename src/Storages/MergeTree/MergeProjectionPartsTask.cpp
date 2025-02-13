#include <Storages/MergeTree/MergeProjectionPartsTask.h>

#include <Common/TransactionID.h>
#include <Storages/MergeTree/MergeList.h>

namespace DB
{

bool MergeProjectionPartsTask::executeStep()
{
    auto & current_level_parts = level_parts[current_level];
    auto & next_level_parts = level_parts[next_level];

    MergeTreeData::MutableDataPartsVector selected_parts;
    while (selected_parts.size() < max_parts_to_merge_in_one_level && !current_level_parts.empty())
    {
        selected_parts.push_back(std::move(current_level_parts.back()));
        current_level_parts.pop_back();
    }

    if (selected_parts.empty())
    {
        if (next_level_parts.empty())
        {
            LOG_WARNING(log, "There is no projection parts merged");

            /// Task is finished
            return false;
        }
        current_level = next_level;
        ++next_level;
    }
    else if (selected_parts.size() == 1)
    {
        if (next_level_parts.empty())
        {
            LOG_DEBUG(log, "Merged a projection part in level {}", current_level);
            selected_parts[0]->renameTo(projection.name + ".proj", true);
            selected_parts[0]->setName(projection.name);
            selected_parts[0]->is_temp = false;
            new_data_part->addProjectionPart(name, std::move(selected_parts[0]));

            /// Task is finished
            return false;
        }

        LOG_DEBUG(log, "Forwarded part {} in level {} to next level", selected_parts[0]->name, current_level);
        next_level_parts.push_back(std::move(selected_parts[0]));
    }
    else if (selected_parts.size() > 1)
    {
        // Generate a unique part name
        ++block_num;
        auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
        MergeTreeData::DataPartsVector const_selected_parts(
            std::make_move_iterator(selected_parts.begin()), std::make_move_iterator(selected_parts.end()));
        projection_future_part->assign(std::move(const_selected_parts));
        projection_future_part->name = fmt::format("{}_{}", projection.name, ++block_num);
        projection_future_part->part_info = {"all", 0, 0, 0};

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        LOG_DEBUG(log, "Merged {} parts in level {} to {}", selected_parts.size(), current_level, projection_future_part->name);
        auto tmp_part_merge_task = mutator->mergePartsToTemporaryPart(
            projection_future_part,
            projection.metadata,
            merge_entry,
            std::make_unique<MergeListElement>((*merge_entry)->table_id, projection_future_part, context),
            *table_lock_holder,
            time_of_merge,
            context,
            space_reservation,
            false, // TODO Do we need deduplicate for projections
            {},
            false, // no cleanup
            projection_merging_params,
            NO_TRANSACTION_PTR,
            /* need_prefix */ true,
            new_data_part.get(),
            ".tmp_proj");

        next_level_parts.push_back(executeHere(tmp_part_merge_task));
        /// FIXME (alesapin) we should use some temporary storage for this,
        /// not commit each subprojection part
        next_level_parts.back()->getDataPartStorage().commitTransaction();
        next_level_parts.back()->is_temp = true;
    }

    /// Need execute again
    return true;
}

}
