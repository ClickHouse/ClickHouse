#include <Processors/DelayedPortsProcessor.h>

#include <base/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

InputPorts createInputPorts(
    const Block & header,
    size_t num_ports,
    IProcessor::PortNumbers delayed_ports,
    bool assert_main_ports_empty)
{
    if (!assert_main_ports_empty)
        return InputPorts(num_ports, header);

    InputPorts res;
    ::sort(delayed_ports.begin(), delayed_ports.end());
    size_t next_delayed_port = 0;
    for (size_t i = 0; i < num_ports; ++i)
    {
        if (next_delayed_port < delayed_ports.size() && i == delayed_ports[next_delayed_port])
        {
            res.emplace_back(header);
            ++next_delayed_port;
        }
        else
            res.emplace_back(Block());
    }

    return res;
}

DelayedPortsProcessor::DelayedPortsProcessor(
    const Block & header, size_t num_ports, const PortNumbers & delayed_ports, bool assert_main_ports_empty)
    : IProcessor(createInputPorts(header, num_ports, delayed_ports, assert_main_ports_empty),
                 OutputPorts((assert_main_ports_empty ? delayed_ports.size() : num_ports), header))
    , num_delayed_ports(delayed_ports.size())
{
    port_pairs.resize(num_ports);
    output_to_pair.reserve(outputs.size());

    for (const auto & delayed : delayed_ports)
        port_pairs[delayed].is_delayed = true;

    auto input_it = inputs.begin();
    auto output_it = outputs.begin();
    for (size_t i = 0; i < num_ports; ++i)
    {
        port_pairs[i].input_port = &*input_it;
        ++input_it;

        if (port_pairs[i].is_delayed || !assert_main_ports_empty)
        {
            port_pairs[i].output_port = &*output_it;
            output_to_pair.push_back(i);
            ++output_it;
        }
    }
}

void DelayedPortsProcessor::finishPair(PortsPair & pair)
{
    if (!pair.is_finished)
    {
        if (pair.output_port)
            pair.output_port->finish();

        pair.input_port->close();

        pair.is_finished = true;
        ++num_finished_pairs;

        if (pair.output_port)
            ++num_finished_outputs;
    }
}

bool DelayedPortsProcessor::processPair(PortsPair & pair)
{
    if (pair.output_port && pair.output_port->isFinished())
    {
        finishPair(pair);
        return false;
    }

    if (pair.input_port->isFinished())
    {
        finishPair(pair);
        return false;
    }

    if (pair.output_port && !pair.output_port->canPush())
        return false;

    pair.input_port->setNeeded();
    if (pair.input_port->hasData())
    {
        if (!pair.output_port)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Input port for DelayedPortsProcessor is assumed to have no data, but it has one");

        pair.output_port->pushData(pair.input_port->pullData(true));
    }

    return true;
}

IProcessor::Status DelayedPortsProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    bool skip_delayed = (num_finished_pairs + num_delayed_ports) < port_pairs.size();
    bool need_data = false;

    if (!are_inputs_initialized && !updated_outputs.empty())
    {
        /// Activate inputs with no output.
        for (const auto & pair : port_pairs)
            if (!pair.output_port)
                pair.input_port->setNeeded();

        are_inputs_initialized = true;
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & pair = port_pairs[output_to_pair[output_number]];

        /// Finish pair of ports earlier if possible.
        if (!pair.is_finished && pair.output_port && pair.output_port->isFinished())
            finishPair(pair);
        else if (!skip_delayed || !pair.is_delayed)
            need_data = processPair(pair) || need_data;
    }

    /// Do not wait for delayed ports if all output ports are finished.
    if (num_finished_outputs == outputs.size())
    {
        for (auto & pair : port_pairs)
            finishPair(pair);

        return Status::Finished;
    }

    for (const auto & input_number : updated_inputs)
    {
        if (!skip_delayed || !port_pairs[input_number].is_delayed)
            need_data = processPair(port_pairs[input_number]) || need_data;
    }

    /// In case if main streams are finished at current iteration, start processing delayed streams.
    if (skip_delayed && (num_finished_pairs + num_delayed_ports) >= port_pairs.size())
    {
        for (auto & pair : port_pairs)
            if (pair.is_delayed)
                need_data = processPair(pair) || need_data;
    }

    if (num_finished_pairs == port_pairs.size())
        return Status::Finished;

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}


NotifyProcessor::NotifyProcessor(const Block & header, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header))
    , aux_in_port(Block(), this)
    , aux_out_port(Block(), this)
{
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

void NotifyProcessor::finishPair(PortsPair & pair)
{
    if (!pair.is_finished)
    {
        pair.output_port->finish();
        pair.input_port->close();

        pair.is_finished = true;
        ++num_finished_pairs;
    }
}

bool NotifyProcessor::processPair(PortsPair & pair)
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
        dataCallback(chunk);
        pair.output_port->push(std::move(chunk));
    }

    return true;
}

IProcessor::Status NotifyProcessor::processRegularPorts(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    bool need_data = false;

    for (const auto & output_number : updated_outputs)
        need_data = processPair(port_pairs[output_number]) || need_data;
    for (const auto & input_number : updated_inputs)
        need_data = processPair(port_pairs[input_number]) || need_data;

    if (num_finished_pairs == port_pairs.size())
        return Status::Finished;

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}

IProcessor::Status NotifyProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    auto status = processRegularPorts(updated_inputs, updated_outputs);
    if (status != Status::Ready)
        return status;

    if (ready == AuxPortState::NotInitialized && isReady())
        ready = AuxPortState::Triggered;

    if (ready == AuxPortState::Triggered)
    {
        if (aux_out_port.canPush())
        {
            aux_out_port.push({});
            ready = AuxPortState::Finished;
            return Status::Ready;
        }
        return Status::PortFull;
    }

    if (waiting == AuxPortState::NotInitialized && isWaiting())
    {
        aux_in_port.setNeeded();
        waiting = AuxPortState::Triggered;
    }

    if (waiting == AuxPortState::Triggered)
    {
        if (aux_in_port.hasData())
        {
            aux_in_port.pull(true);
            waiting = AuxPortState::Finished;
            return Status::Ready;
        }
        return Status::PortFull;
    }

    return Status::Ready;
}

std::pair<InputPort *, OutputPort *> NotifyProcessor::getAuxPorts()
{
    return std::make_pair(&aux_in_port, &aux_out_port);
}


}
