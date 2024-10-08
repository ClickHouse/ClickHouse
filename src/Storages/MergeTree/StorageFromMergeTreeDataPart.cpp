#include <DataTypes/ObjectUtils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool materialize_ttl_recalculate_only;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

bool StorageFromMergeTreeDataPart::materializeTTLRecalculateOnly() const
{
    if (parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "parts must not be empty for materializeTTLRecalculateOnly");
    return (*parts.front()->storage.getSettings())[MergeTreeSetting::materialize_ttl_recalculate_only];
}

void StorageFromMergeTreeDataPart::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    query_plan.addStep(MergeTreeDataSelectExecutor(storage).readFromParts(
        parts,
        mutations_snapshot,
        column_names,
        storage_snapshot,
        query_info,
        context,
        max_block_size,
        num_streams,
        nullptr,
        analysis_result_ptr));
}

StorageSnapshotPtr
StorageFromMergeTreeDataPart::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const
{
    const auto & storage_columns = metadata_snapshot->getColumns();
    if (!hasDynamicSubcolumns(storage_columns))
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot);

    auto data_parts = storage.getDataPartsVectorForInternalUsage();

    auto object_columns = getConcreteObjectColumns(
        data_parts.begin(), data_parts.end(), storage_columns, [](const auto & part) -> const auto & { return part->getColumns(); });

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(object_columns));
}

}
