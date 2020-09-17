#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPipeline.h>

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
    StreamLocalLimits limits_,
    std::shared_ptr<const EnabledQuota> quota_,
    std::shared_ptr<Context> context_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , storage(std::move(storage_))
    , table_lock(std::move(table_lock_))
    , limits(std::move(limits_))
    , quota(std::move(quota_))
    , context(std::move(context_))
{
}

void SettingQuotaAndLimitsStep::transformPipeline(QueryPipeline & pipeline)
{
    /// Table lock is stored inside pipeline here.
    pipeline.addTableLock(table_lock);

    pipeline.setLimits(limits);

    if (quota)
        pipeline.setQuota(quota);

    pipeline.addInterpreterContext(std::move(context));
    pipeline.addStorageHolder(std::move(storage));
}

}
