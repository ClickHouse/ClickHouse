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

    while (status == Status::Ready)
    {
        switch (planning_status)
        {
            case PlanningStatus::INIT:
            {
                status = init();
                break;
            }
            case PlanningStatus::READ_IF_CAN:
            {
                status = prepareConsume();
                break;
            }
            case PlanningStatus::WAIT_IN:
            {
                status = waitForDataIn();
                break;
            }
            case PlanningStatus::WAIT_OUT_AND_PUSH:
            {
                status = prepareSend();
                break;
            }
            case PlanningStatus::PUSH:
            {
                status = prepareSend();
                break;
            }
            case PlanningStatus::WAIT_OUT_FLUSH:
            {
                status = prepareSendFlush();
                break;
            }
            case FINISH:
            {
                status = finish();
                break;
            }
        }
    }

    return status;
}

IProcessor::Status PlanSquashingTransform::init()
{
    for (auto input : inputs)
    {
        input.setNeeded();
        if (input.hasData())
            available_inputs++;
    }

    planning_status = PlanningStatus::READ_IF_CAN;
    return Status::Ready;
}

IProcessor::Status PlanSquashingTransform::prepareConsume()
{
    if (available_inputs == 0)
    {
        planning_status = PlanningStatus::WAIT_IN;
        return Status::NeedData;
    }
    finished = false;

    bool inputs_have_no_data = true;
    for (auto & input : inputs)
    {
        if (input.hasData())
        {
            inputs_have_no_data = false;
            chunk = input.pull();
            transform(chunk);

            available_inputs--;
            if (chunk.hasChunkInfo())
            {
                planning_status = PlanningStatus::WAIT_OUT_AND_PUSH;
                return Status::Ready;
            }
        }

        if (available_inputs == 0)
        {
            planning_status = PlanningStatus::WAIT_IN;
            return Status::NeedData;
        }
    }

    if (inputs_have_no_data)
    {
        if (checkInputs())
            return Status::Ready;

        planning_status = PlanningStatus::WAIT_IN;
        return Status::NeedData;
    }
    return Status::Ready;
}

bool PlanSquashingTransform::checkInputs()
{
    bool all_finished = true;

    for (auto & output : outputs)
    {
        if (!output.isFinished())
            all_finished = false;
    }
    if (all_finished) /// If all outputs are closed, we close inputs (just in case)
    {
        planning_status = PlanningStatus::FINISH;
        return true;
    }

    all_finished = true;
    for (auto & input : inputs)
    {
        
        if (!input.isFinished())
            all_finished = false;
    }

    if (all_finished) /// If all inputs are closed, we check if we have data in balancing
    {
        if (balance.isDataLeft()) /// If we have data in balancing, we process this data
        {
            planning_status = PlanningStatus::WAIT_OUT_FLUSH;
            finished = false;
            transform(chunk);
        }
        else    /// If we don't have data, We send FINISHED
            planning_status = PlanningStatus::FINISH;
        return true;
    }
    return false;
}

IProcessor::Status PlanSquashingTransform::waitForDataIn()
{
    bool all_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;

        all_finished = false;

        if (!input.hasData())
            continue;
        
        available_inputs++;
    }
    if (all_finished)
    {
        checkInputs();
        return Status::Ready;
    }
    
    if (available_inputs > 0)
    {
        planning_status = PlanningStatus::READ_IF_CAN;
        return Status::Ready;
    }
    
    return Status::NeedData;
}

void PlanSquashingTransform::transform(Chunk & chunk_)
{
    if (!finished)
    {
        Chunk res_chunk = balance.add(std::move(chunk_));
        std::swap(res_chunk, chunk_);
    }
    else
    {
        Chunk res_chunk = balance.add({});
        std::swap(res_chunk, chunk_);
    }
}

IProcessor::Status PlanSquashingTransform::prepareSend()
{
    if (!chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should be available in prepareSend");

    for (auto &output : outputs)
    {

        if (output.canPush())
        {
            planning_status = PlanningStatus::READ_IF_CAN;
            output.push(std::move(chunk));
            return Status::Ready;
        }
    }
    return Status::PortFull;
}

IProcessor::Status PlanSquashingTransform::prepareSendFlush()
{
    if (!chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should be available in prepareSendFlush");

    for (auto &output : outputs)
    {

        if (output.canPush())
        {
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
