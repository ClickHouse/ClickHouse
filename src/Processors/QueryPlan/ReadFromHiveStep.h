#pragma once
#include "config.h"

#if USE_HIVE
#include <Processors/QueryPlan/ReadFromPreparedSource.h>

namespace DB
{

struct HiveSelectAnalysisResult
{
    /// Number of partitions before partition pruning.
    std::atomic<UInt64> partitions_before_prune{0};
    /// Number of partitions after partition pruning.
    std::atomic<UInt64> partitions_after_prune{0};
    /// Number of files before file pruning.
    std::atomic<UInt64> files_before_prune{0};
    /// Number of files after file pruning.
    std::atomic<UInt64> files_after_prune{0};

    Object toObject() const
    {
        return {
            {"partitions_before_prune", partitions_before_prune.load(std::memory_order_relaxed)},
            {"partitions_after_prune", partitions_after_prune.load(std::memory_order_relaxed)},
            {"files_before_prune", files_before_prune.load(std::memory_order_relaxed)},
            {"files_after_prune", files_after_prune.load(std::memory_order_relaxed)},
        };
    }
};
using HiveSelectAnalysisResultPtr = std::shared_ptr<HiveSelectAnalysisResult>;

class ReadFromHiveStep : public ReadFromStorageStep
{
public:
    ReadFromHiveStep(
        Pipe pipe_,
        String storage_name,
        std::shared_ptr<const StorageLimitsList> storage_limits,
        StorageID storage_id_,
        HiveSelectAnalysisResultPtr analysis_result_)
        : ReadFromStorageStep(std::move(pipe_), std::move(storage_name), std::move(storage_limits))
        , storage_id(std::move(storage_id_))
        , analysis_result(std::move(analysis_result_))
    {
        setStepDescription(storage_id.getFullNameNotQuoted());
    }

    String getName() const override { return "ReadFromHive"; }

    void updateEstimate(MutableColumns & columns) const override;

private:
    const StorageID storage_id;
    HiveSelectAnalysisResultPtr analysis_result;
};

}
#endif
