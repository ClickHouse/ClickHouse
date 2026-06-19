#include <Processors/QueryPlan/MergeTreeFinalMerge.h>

#include <Interpreters/ExpressionActions.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CoalescingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Transforms/SelectByIndicesTransform.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <ctime>

namespace DB
{

void addMergingFinal(
    Pipe & pipe,
    const SortDescription & sort_description,
    MergeTreeData::MergingParams merging_params,
    const StorageMetadataPtr & metadata_snapshot,
    size_t max_block_size_rows,
    bool enable_vertical_final)
{
    auto header = pipe.getSharedHeader();
    size_t num_outputs = pipe.numOutputPorts();

    auto now = time(nullptr);

    auto get_merging_processor = [&]() -> MergingTransformPtr
    {
        switch (merging_params.mode)
        {
            case MergeTreeData::MergingParams::Ordinary:
                return std::make_shared<MergingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, SortingQueueStrategy::Batch);

            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, true, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt);

            case MergeTreeData::MergingParams::Summing: {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, required_columns, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.allow_tuple_element_aggregation);
            }

            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.allow_tuple_element_aggregation);

            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.is_deleted_column, merging_params.version_column, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, /*out_row_sources_buf_*/ nullptr, /*use_average_block_sizes*/ false, /*cleanup*/ !merging_params.is_deleted_column.empty(), enable_vertical_final);

            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt);

            case MergeTreeData::MergingParams::Graphite:
                return std::make_shared<GraphiteRollupSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.graphite_params, now);

            case MergeTreeData::MergingParams::Coalescing:
            {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<CoalescingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, required_columns, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.allow_tuple_element_aggregation);
            }
        }
    };

    pipe.addTransform(get_merging_processor());
    if (enable_vertical_final)
        pipe.addSimpleTransform([](const SharedHeader & header_)
                                { return std::make_shared<SelectByIndicesTransform>(header_); });
}

}
