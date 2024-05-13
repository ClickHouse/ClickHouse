#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/IProcessor.h>
#include <Interpreters/Squashing.h>

enum PlanningStatus
{
    INIT,
    READ_IF_CAN,
    WAIT_IN,
    WAIT_OUT_AND_PUSH,
    WAIT_OUT_FLUSH,
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
    Status init();
    Status prepareConsume();
    Status prepareSend();
    Status prepareSendFlush();
    Status waitForDataIn();
    Status finish();

    bool checkInputs();
    void transform(Chunk & chunk);

protected:

private:
    Chunk chunk;
    PlanSquashing balance;
    PlanningStatus planning_status = PlanningStatus::INIT;
    size_t available_inputs = 0;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};
}

