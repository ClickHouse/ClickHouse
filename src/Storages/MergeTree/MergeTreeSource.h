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
    explicit MergeTreeSource(MergeTreeSelectProcessorPtr processor_, const std::string & log_name_);
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
    /// The tracing span name of `tryGenerate`, formatted once instead of on every chunk.
    const std::string span_operation_name;

#if defined(OS_LINUX)
    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;
#endif

    Chunk processReadResult(ChunkAndProgress chunk);
};

}
