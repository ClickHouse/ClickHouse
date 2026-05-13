#include <Processors/PingPongProcessor.h>

#include <Core/Block.h>
#include <Processors/Port.h>

namespace DB
{

/// Create list with `num_ports` of regular ports and 1 auxiliary port with empty header.
template <typename T> requires std::is_same_v<T, InputPorts> || std::is_same_v<T, OutputPorts>
static T createPortsWithExtra(const Block & header, size_t num_ports)
{
    T res(num_ports, header);
    res.emplace_back(Block());
    return res;
}

PingPongProcessor::PingPongProcessor(const Block & header, size_t num_ports, Order order_)
    : IProcessor(createPortsWithExtra<InputPorts>(header, num_ports),
                 createPortsWithExtra<OutputPorts>(header, num_ports))
    , aux_in_port(inputs.back())
    , aux_out_port(outputs.back())
    , order(order_)
{
    assert(order == First || order == Second);

    port_pairs.resize(num_ports);

    auto input_it = inputs.begin();
    auto output_it = outputs.begin();
    for (size_t i = 0; i < num_ports; ++i)
    {
        port_pairs[i].input_port = &*input_it;
        ++input_it;

        port_pairs[i].output_port = &*output_it;
        ++output_it;
    }
}

void PingPongProcessor::finishPair(PortsPair & pair)
{
    if (!pair.is_finished)
    {
        pair.output_port->finish();
        pair.input_port->close();

        pair.is_finished = true;
        ++num_finished_pairs;
    }
}

bool PingPongProcessor::processPair(PortsPair & pair)
{
    if (pair.output_port->isFinished())
    {
        finishPair(pair);
        return false;
    }

    if (pair.input_port->isFinished())
    {
        finishPair(pair);
        return false;
    }

    if (!pair.output_port->canPush())
    {
        pair.input_port->setNotNeeded();
        return false;
    }

    pair.input_port->setNeeded();
    if (pair.input_port->hasData())
    {
        Chunk chunk = pair.input_port->pull(true);
        ready_to_send |= consume(chunk);
        pair.output_port->push(std::move(chunk));
    }

    return true;
}

bool PingPongProcessor::isPairsFinished() const
{
    return num_finished_pairs == port_pairs.size();
}

IProcessor::Status PingPongProcessor::processRegularPorts()
{
    if (isPairsFinished())
        return Status::Finished;

    bool need_data = false;

    for (auto & pair : port_pairs)
        need_data = processPair(pair) || need_data;

    if (isPairsFinished())
        return Status::Finished;

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}

bool PingPongProcessor::sendPing()
{
    if (aux_out_port.canPush())
    {
        Chunk chunk(aux_out_port.getHeader().cloneEmpty().getColumns(), 0);
        aux_out_port.push(std::move(chunk));
        is_send = true;
        aux_out_port.finish();
        return true;
    }
    return false;
}

bool PingPongProcessor::recievePing()
{
    if (aux_in_port.hasData())
    {
        aux_in_port.pull();
        is_received = true;
        aux_in_port.close();
        return true;
    }
    return false;
}

bool PingPongProcessor::canSend() const
{
    return !is_send && (ready_to_send || isPairsFinished());
}

IProcessor::Status PingPongProcessor::prepare()
{
    if (!set_needed_once && !is_received && !aux_in_port.isFinished())
    {
        set_needed_once = true;
        aux_in_port.setNeeded();
    }

    if (order == First || is_send)
    {
        if (!is_received)
        {
            bool received = recievePing();
            if (!received)
            {
                return Status::NeedData;
            }
        }
    }

    if (order == Second || is_received)
    {
        if (!is_send && canSend())
        {
            bool sent = sendPing();
            if (!sent)
                return Status::PortFull;
        }
    }

    auto status = processRegularPorts();
    if (status == Status::Finished)
    {
        if (order == First || is_send)
        {
            if (!is_received)
            {
                bool received = recievePing();
                if (!received)
                {
                    return Status::NeedData;
                }
            }
        }

        if (order == Second || is_received)
        {
            if (!is_send && canSend())
            {
                bool sent = sendPing();
                if (!sent)
                    return Status::PortFull;
            }
        }
    }
    return status;
}

std::pair<InputPort *, OutputPort *> PingPongProcessor::getAuxPorts()
{
    return std::make_pair(&aux_in_port, &aux_out_port);
}

bool ReadHeadBalancedProcessor::consume(const Chunk & chunk)
{
    data_consumed += chunk.getNumRows();
    return data_consumed > size_to_wait;
}

}
