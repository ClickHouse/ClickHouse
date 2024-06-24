#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::PlanSquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header)), squashing(header, min_block_size_rows, min_block_size_bytes)
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
                return prepareConsume();
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
    bool all_finished = true;
    for (auto & input : inputs)
    {
        if (!input.isFinished())
        {
            all_finished = false;
            input.setNeeded();
        }
        else
            continue;

        if (input.hasData())
        {
            chunk = input.pull();
            chunk = transform(std::move(chunk));

            if (chunk.hasChunkInfo())
            {
                planning_status = PlanningStatus::PUSH;
                return Status::Ready;
            }
        }
    }

    if (all_finished) /// If all inputs are closed, we check if we have data in balancing
    {
        if (squashing.isDataLeft()) /// If we have data in balancing, we process this data
        {
            planning_status = PlanningStatus::FLUSH;
            chunk = flushChunk();
            return Status::Ready;
        }
        planning_status = PlanningStatus::FINISH;
        return Status::Ready;
    }

    return Status::NeedData;
}

Chunk PlanSquashingTransform::transform(Chunk && chunk_)
{
    return squashing.add(std::move(chunk_));
}

Chunk PlanSquashingTransform::flushChunk()
{
    return squashing.flush();
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
            else
                planning_status = PlanningStatus::FINISH;

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
