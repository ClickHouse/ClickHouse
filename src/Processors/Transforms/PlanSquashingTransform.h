#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/IProcessor.h>
#include <Interpreters/Squashing.h>
#include "Processors/Port.h"

enum PlanningStatus
{
    INIT,
    READ_IF_CAN,
    WAIT_IN,
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

    void transform(Chunk & chunk);
    void flushChunk();

private:
    Chunk chunk;
    PlanSquashing balance;
    PlanningStatus planning_status = PlanningStatus::INIT;
};
}

