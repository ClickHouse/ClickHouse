#pragma once
#include <Processors/ISource.h>

namespace DB
{

class MergeTreeSelectProcessor;
using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

struct ChunkAndProgress;

class MergeTreeSource final : public ISource
{
public:
    enum class PartialResultMode
    {
        StopReading,
        Drain,
    };

    explicit MergeTreeSource(
        MergeTreeSelectProcessorPtr processor_, const std::string & log_name_, PartialResultMode partial_result_mode_);
    ~MergeTreeSource() override;

    std::string getName() const override;

    Status prepare() override;
    void cancel(CancelReason reason) noexcept override;

#if defined(OS_LINUX)
    int schedule() override;
#endif

protected:
    std::optional<Chunk> tryGenerate() override;

    void onPartialResult() noexcept override;
    void onCancel() noexcept override;

private:
    MergeTreeSelectProcessorPtr processor;
    const std::string log_name;
    const PartialResultMode partial_result_mode;

#if defined(OS_LINUX)
    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;
#endif

    Chunk processReadResult(ChunkAndProgress chunk);
};

}
