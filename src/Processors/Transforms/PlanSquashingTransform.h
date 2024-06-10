#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/IProcessor.h>
#include <Interpreters/Squashing.h>

enum PlanningStatus
{
    INIT,
    READ_IF_CAN,
    PUSH,
    FLUSH,
    FINISH
};

namespace DB
{

class PlanSquashingTransform : public IProcessor
{
public:
    PlanSquashingTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports);

    String getName() const override { return "PlanSquashingTransform"; }

    InputPorts & getInputPorts() { return inputs; }
    OutputPorts & getOutputPorts() { return outputs; }

    Status prepare() override;
    void work() override;
    void init();
    Status prepareConsume();
    Status sendOrFlush();
    Status waitForDataIn();
    Status finish();

    Chunk transform(Chunk && chunk);
    Chunk flushChunk();

private:
    Chunk chunk;
    Squashing squashing;
    PlanningStatus planning_status = PlanningStatus::INIT;
};
}

