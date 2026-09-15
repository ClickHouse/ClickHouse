#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/RuntimeFilterReadRangesRefiner.h>

namespace DB
{

class MergeTreeSelectProcessor;
using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

/// Reads from MergeTree like MergeTreeSource, but only after receiving a "seal" chunk through
/// its input port (see BuildRuntimeFilterTransform::addSealPort): the seal signals that the
/// build side of a JOIN is complete, and carries the runtime filter which is handed to the
/// ranges refiner of the read pool before the first read. Gating is expressed as an ordinary
/// pipeline edge, so the executor sees the dependency and never schedules the read early.
///
/// Fail-open: if the input finishes without a seal (query cancellation, or a join pipeline
/// which did not wire the seal), reading proceeds without the filter, which is always correct.
class SealGatedReadTransform final : public IProcessor
{
public:
    SealGatedReadTransform(
        SharedHeader header,
        MergeTreeSelectProcessorPtr processor_,
        std::shared_ptr<RuntimeFilterReadRangesRefiner> refiner_,
        std::string log_name_);

    ~SealGatedReadTransform() override;

    String getName() const override { return "SealGatedRead"; }

    Status prepare() override;
    void work() override;

    InputPort & getSealInput() { return inputs.front(); }

    std::optional<ReadProgress> getReadProgress() override;
    void addTotalRowsApprox(size_t value);
    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;

protected:
    void onCancel() noexcept override;

private:
    const MergeTreeSelectProcessorPtr processor;
    const std::shared_ptr<RuntimeFilterReadRangesRefiner> refiner;
    const std::string log_name;

    Chunk seal;
    Chunk ready_chunk;
    bool reading = false;
    bool finished = false;

    std::mutex read_progress_mutex;
    ReadProgressCounters read_progress;
    std::shared_ptr<const StorageLimitsList> storage_limits;
};

}
