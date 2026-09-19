#include <Processors/Transforms/DistinctLimitsCheckingTransform.h>
#include <Processors/Transforms/DistinctSetMemoryTracker.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctLimitsCheckingTransform::DistinctLimitsCheckingTransform(const SharedHeader & header, const SizeLimits & size_limits_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header), OutputPorts(num_streams, header))
    , size_limits(size_limits_)
{
    port_pairs.reserve(num_streams);
    port_to_pair.reserve(2 * num_streams);
    auto output = outputs.begin();
    for (auto & input : inputs)
    {
        auto & pair = port_pairs.emplace_back(input, *output++);
        port_to_pair.emplace(&pair.input, &pair);
        port_to_pair.emplace(&pair.output, &pair);
    }
}

IProcessor::Status DistinctLimitsCheckingTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    bool has_full_port = false;
    auto prepare_ports = [&](const auto & updated_ports)
    {
        for (const auto * port : updated_ports)
        {
            /// `BREAK` emits the chunk that reaches the limit and stops processing further port updates.
            if (limit_reached)
                break;

            auto & pair = *port_to_pair.at(port);
            const auto status = preparePair(pair);
            if (status == Status::Finished && !pair.is_finished)
            {
                pair.is_finished = true;
                ++num_finished_port_pairs;
            }
            has_full_port |= status == Status::PortFull;
        }
    };

    prepare_ports(updated_inputs);
    prepare_ports(updated_outputs);

    if (limit_reached)
    {
        for (auto & input : inputs)
            input.close();
        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    if (num_finished_port_pairs == port_pairs.size())
        return Status::Finished;

    return has_full_port ? Status::PortFull : Status::NeedData;
}

IProcessor::Status DistinctLimitsCheckingTransform::prepare()
{
    chassert(port_pairs.size() == 1);
    return prepare({&port_pairs.front().input}, {&port_pairs.front().output});
}

IProcessor::Status DistinctLimitsCheckingTransform::preparePair(PortPair & pair)
{
    auto & input = pair.input;
    auto & output = pair.output;

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto data_chunk = input.pullData(true);
    auto memory_usage = data_chunk.chunk.getChunkInfos().extract<DistinctSetMemoryUsage>();
    chassert(!data_chunk.chunk.hasRows() || memory_usage);
    if (memory_usage)
    {
        rows += data_chunk.chunk.getNumRows();
        limit_reached = !size_limits.check(rows, memory_usage->total_bytes, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
    if (!data_chunk.exception && !data_chunk.chunk.hasRows() && data_chunk.chunk.getChunkInfos().empty())
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }
        input.setNeeded();
        return Status::NeedData;
    }
    output.pushData(std::move(data_chunk));
    return Status::PortFull;
}

}
