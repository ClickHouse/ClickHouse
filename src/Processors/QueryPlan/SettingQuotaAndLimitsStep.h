#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/TableLockHolder.h>
#include <DataStreams/StreamLocalLimits.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class EnabledQuota;

/// Add limits, quota, table_lock and other stuff to pipeline.
/// Doesn't change DataStream.
class SettingQuotaAndLimitsStep : public ITransformingStep
{
public:
    SettingQuotaAndLimitsStep(
        const DataStream & input_stream_,
        StoragePtr storage_,
        TableLockHolder table_lock_,
        StreamLocalLimits & limits_,
        SizeLimits & leaf_limits_,
        std::shared_ptr<const EnabledQuota> quota_,
        std::shared_ptr<Context> context_);

    String getName() const override { return "SettingQuotaAndLimits"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    std::shared_ptr<Context> context;
    StoragePtr storage;
    TableLockHolder table_lock;
    StreamLocalLimits limits;
    SizeLimits leaf_limits;
    std::shared_ptr<const EnabledQuota> quota;
};

}
