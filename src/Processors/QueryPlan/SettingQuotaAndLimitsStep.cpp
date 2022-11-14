#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

SettingQuotaAndLimitsStep::SettingQuotaAndLimitsStep(
    const DataStream & input_stream_,
    StoragePtr storage_,
    TableLockHolder table_lock_,
    StreamLocalLimits & limits_,
    SizeLimits & leaf_limits_,
    std::shared_ptr<const EnabledQuota> quota_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , context(std::move(context_))
    , storage(std::move(storage_))
    , table_lock(std::move(table_lock_))
    , limits(limits_)
    , leaf_limits(leaf_limits_)
    , quota(std::move(quota_))
{
}

void SettingQuotaAndLimitsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// Table lock is stored inside pipeline here.
    pipeline.setLimits(limits);

    /**
      * Leaf size limits should be applied only for local processing of distributed queries.
      * Such limits allow to control the read stage on leaf nodes and exclude the merging stage.
      * Consider the case when distributed query needs to read from multiple shards. Then leaf
      * limits will be applied on the shards only (including the root node) but will be ignored
      * on the results merging stage.
      */
    if (!storage->isRemote())
        pipeline.setLeafLimits(leaf_limits);

    if (quota)
        pipeline.setQuota(quota);

    /// Order of resources below is important.
    if (context)
        pipeline.addInterpreterContext(std::move(context));

    if (storage)
        pipeline.addStorageHolder(std::move(storage));

    if (table_lock)
        pipeline.addTableLock(std::move(table_lock));
}

}
