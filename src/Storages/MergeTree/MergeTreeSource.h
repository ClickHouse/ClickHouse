#pragma once
#include <Processors/ISource.h>

#include <IO/Progress.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace DB
{

class MergeTreeSelectProcessor;
using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

struct ChunkAndProgress;

using DataflowStatisticsCallback = std::function<void(const RuntimeDataflowStatistics & statistics)>;

class MergeTreeSource final : public ISource
{
public:
    MergeTreeSource(MergeTreeSelectProcessorPtr processor_, const std::string & log_name_, UpdaterPtr updater_);
    ~MergeTreeSource() override;

    std::string getName() const override;

    Status prepare() override;

#if defined(OS_LINUX)
    int schedule() override;
#endif

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() noexcept override;

private:
    MergeTreeSelectProcessorPtr processor;
    const std::string log_name;

    RuntimeDataflowStatistics statistics{};
    UpdaterPtr updater;

#if defined(OS_LINUX)
    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;
#endif

    Chunk processReadResult(ChunkAndProgress chunk);
};

}
