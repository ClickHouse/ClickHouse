#include <Storages/KeyDescriptionMonotonicityChecker.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartitionCompatibilityVerifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    auto hyperrectangle = Range(source_table_info.min_idx, true, source_table_info.max_idx, true);
    auto block_with_min_and_max_idx = IMergeTreeDataPart::MinMaxIndex::buildBlockWithMinAndMaxIndexes(
        source_table_info.storage,
        {hyperrectangle}
    );

    MergeTreePartition().createAndValidateMinMaxPartitionIds(metadata, block_with_min_and_max_idx, context);
}

}
