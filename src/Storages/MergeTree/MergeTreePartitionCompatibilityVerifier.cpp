#include <Storages/KeyDescriptionMonotonicityChecker.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartitionCompatibilityVerifier.h>

namespace DB
{

namespace
{
    Block buildBlockWithMinAndMaxIdx(const MergeTreeData & data, const Field & min, const Field & max)
    {
        Block block;

        auto metadata_snapshot = data.getInMemoryMetadataPtr();
        const auto & partition_key = metadata_snapshot->getPartitionKey();

        auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
        auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);
        size_t minmax_idx_size = minmax_column_types.size();

        assert(minmax_idx_size == 1u);

        for (size_t i = 0; i < minmax_idx_size; ++i)
        {
            auto data_type = minmax_column_types[i];
            auto column_name = minmax_column_names[i];

            auto column = data_type->createColumn();

            column->insert(min);
            column->insert(max);

            auto column_with_type_and_name = ColumnWithTypeAndName(column->getPtr(), data_type, column_name);

            block.insert(column_with_type_and_name);
        }

        return block;
    }
}

void MergeTreePartitionCompatibilityVerifier::verify(
    const SourceTableInfo & source_table_info,
    const StorageMetadataPtr & destination_table_metadata,
    ContextPtr context
)
{
    if (!isDestinationPartitionExpressionMonotonicallyIncreasing(source_table_info, destination_table_metadata, context))
    {
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Destination table partition expression is not monotonically increasing");
    }

    validatePartitionIds(source_table_info, destination_table_metadata, context);
}

bool MergeTreePartitionCompatibilityVerifier::isDestinationPartitionExpressionMonotonicallyIncreasing(
    const SourceTableInfo & source_table_info,
    const StorageMetadataPtr & destination_table_metadata,
    ContextPtr context
)
{
    auto range = Range(source_table_info.min_idx, true, source_table_info.max_idx, true);

    auto monotonicity_info = KeyDescriptionMonotonicityChecker::getMonotonicityInfo(destination_table_metadata->getPartitionKey(), range, context);

    return monotonicity_info.is_monotonic && monotonicity_info.is_positive;
}

void MergeTreePartitionCompatibilityVerifier::validatePartitionIds(
    const SourceTableInfo & source_table_info,
    const StorageMetadataPtr & metadata,
    ContextPtr context
)
{
    auto block_with_min_and_max_idx = buildBlockWithMinAndMaxIdx(source_table_info.storage, source_table_info.min_idx, source_table_info.max_idx);

    MergeTreePartition().createAndValidateMinMaxPartitionIds(metadata, block_with_min_and_max_idx, context);
}

}
