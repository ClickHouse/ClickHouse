#pragma once

#include <Processors/Sources/SourceWithProgress.h>

#include <Storages/IStorage.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class PushingSource : public SourceWithProgress
{
    friend class PushingPipelineExecutor;

public:
    PushingSource(Block sample) : SourceWithProgress(sample), column_names(sample.getNames()) { }

    String getName() const override { return "Pushing"; }

    Status prepare() override;

    void push(const Block & block);

protected:
    Chunk generate() override
    {
        //std::cerr << "generating\n";
        pushed_input = false;
        return std::move(chunk);
    }

private:
    Names column_names;
    Chunk chunk;
    std::atomic_bool no_input_flag = true;
    bool pushed_input = false;
};

}
