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
    std::span<ISpillable * const> getSpillables() override;

    Status prepare() override;

    /// The source of the lazy branch of lazy materialization reads the rows already selected by the main branch,
    /// and a partial result still needs them, so such a source must not stop on `CancelReason::PartialResult`.
    void keepReadingOnPartialResult() { keep_reading_on_partial_result = true; }

    void cancel(CancelReason reason) noexcept override;
    using ISource::cancel;

#if defined(OS_LINUX)
    int schedule() override;
#endif

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() noexcept override;

private:
    MergeTreeSelectProcessorPtr processor;
    /// Formatted once: `tryGenerate` is called for every block, and the span is usually not recorded.
    const std::string span_name;
    bool keep_reading_on_partial_result = false;

#if defined(OS_LINUX)
    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;
#endif

    Chunk processReadResult(ChunkAndProgress chunk);
};

}
