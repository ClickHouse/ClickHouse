#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

namespace DB
{

PlanSquashingTransform::PlanSquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header)), balance(header, min_block_size_rows, min_block_size_bytes)
{
}

IProcessor::Status PlanSquashingTransform::prepare()
{
    Status status = Status::Ready;

    while (planning_status != PlanningStatus::FINISH)
    {
        switch (planning_status)
        {
            case INIT:
                init();
                break;
            case READ_IF_CAN:
                status = prepareConsume();
                break;
            case WAIT_IN:
                planning_status = PlanningStatus::READ_IF_CAN;
                return Status::NeedData;
            case PUSH:
                return sendOrFlush();
            case FLUSH:
                return sendOrFlush();
            case FINISH:
                break; /// never reached
        }
    }
    if (status == Status::Ready)
        status = finish();
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There should be a Ready status to finish the PlanSquashing");

    return status;
}

void PlanSquashingTransform::work()
{
    prepare();
}

void PlanSquashingTransform::init()
{
    for (auto input: inputs)
        if (!input.isFinished())
            input.setNeeded();

    planning_status = PlanningStatus::READ_IF_CAN;
}

IProcessor::Status PlanSquashingTransform::prepareConsume()
{
    bool inputs_have_no_data = true, all_finished = true;
    for (auto & input : inputs)
    {
        if (!input.isFinished())
            all_finished = false;

        if (input.hasData())
        {
            inputs_have_no_data = false;
            chunk = input.pull();
            transform(chunk);

            if (chunk.hasChunkInfo())
            {
                planning_status = PlanningStatus::PUSH;
                return Status::Ready;
            }
        }
    }

    if (all_finished) /// If all inputs are closed, we check if we have data in balancing
    {
        if (balance.isDataLeft()) /// If we have data in balancing, we process this data
        {
            planning_status = PlanningStatus::FLUSH;
            flushChunk();
        }
        planning_status = PlanningStatus::PUSH;
        return Status::Ready;
    }

    if (inputs_have_no_data)
        planning_status = PlanningStatus::WAIT_IN;

    return Status::Ready;
}

IProcessor::Status PlanSquashingTransform::waitForDataIn()
{
    bool all_finished = true;
    bool inputs_have_no_data = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;

        all_finished = false;

        if (input.hasData())
            inputs_have_no_data = false;

    }
    if (all_finished)
    {
        planning_status = PlanningStatus::READ_IF_CAN;
        return Status::Ready;
    }

    if (!inputs_have_no_data)
    {
        planning_status = PlanningStatus::READ_IF_CAN;
        return Status::Ready;
    }

    return Status::NeedData;
}

void PlanSquashingTransform::transform(Chunk & chunk_)
{
    Chunk res_chunk = balance.add(std::move(chunk_));
    std::swap(res_chunk, chunk_);
}

void PlanSquashingTransform::flushChunk()
{
    Chunk res_chunk = balance.flush();
    std::swap(res_chunk, chunk);
}

IProcessor::Status PlanSquashingTransform::sendOrFlush()
{
    if (!chunk)
    {
        planning_status = PlanningStatus::FINISH;
        return Status::Ready;
    }

    for (auto &output : outputs)
    {
        if (output.canPush())
        {
            if (planning_status == PlanningStatus::PUSH)
                planning_status = PlanningStatus::READ_IF_CAN;

            output.push(std::move(chunk));
            return Status::Ready;
        }
    }
    return Status::PortFull;
}

IProcessor::Status PlanSquashingTransform::finish()
{
    for (auto & in : inputs)
        in.close();
    for (auto & output : outputs)
        output.finish();

    return Status::Finished;
}
}
