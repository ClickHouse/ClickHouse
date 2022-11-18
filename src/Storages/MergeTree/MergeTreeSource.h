#pragma once
#include <Processors/ISource.h>

namespace DB
{

class IMergeTreeSelectAlgorithm;
using MergeTreeSelectAlgorithmPtr = std::unique_ptr<IMergeTreeSelectAlgorithm>;

struct ChunkAndProgress;

class MergeTreeSource final : public ISource
{
public:
    explicit MergeTreeSource(MergeTreeSelectAlgorithmPtr algorithm_);
    ~MergeTreeSource() override;

    std::string getName() const override;

    Status prepare() override;
    int schedule() override;

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() override;

private:
    MergeTreeSelectAlgorithmPtr algorithm;

    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;

    std::optional<Chunk> reportProgress(ChunkAndProgress chunk);
};

}
