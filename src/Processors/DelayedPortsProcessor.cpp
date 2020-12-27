#include <Processors/DelayedPortsProcessor.h>

namespace DB
{

DelayedPortsProcessor::DelayedPortsProcessor(
    const Block & header, size_t num_ports, const PortNumbers & delayed_ports, bool assert_delayed_ports_empty)
    : IProcessor(InputPorts(num_ports, header),
                 OutputPorts(num_ports - (assert_delayed_ports_empty ? delayed_ports.size() : 0), header))
    , num_delayed(delayed_ports.size())
{
    port_pairs.resize(num_ports);

    for (const auto & delayed : delayed_ports)
        port_pairs[delayed].is_delayed = true;

    auto input_it = inputs.begin();
    auto output_it = outputs.begin();
    for (size_t i = 0; i < num_ports; ++i)
    {
        port_pairs[i].input_port = &*input_it;
        ++input_it;

        if (!port_pairs[i].is_delayed || !assert_delayed_ports_empty)
        {
            port_pairs[i].output_port = &*output_it;
            ++output_it;
        }
    }
}

bool DelayedPortsProcessor::processPair(PortsPair & pair)
{
    auto finish = [&]()
    {
        if (!pair.is_finished)
        {
            pair.is_finished = true;
            ++num_finished;
        }
    };

    if (pair.output_port && pair.output_port->isFinished())
    {
        pair.input_port->close();
        finish();
        return false;
    }

    if (pair.input_port->isFinished())
    {
        if (pair.output_port)
            pair.output_port->finish();
        finish();
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

        pair.output_port->pushData(pair.input_port->pullData());
    }

    return true;
}

IProcessor::Status DelayedPortsProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    bool skip_delayed = (num_finished + num_delayed) < port_pairs.size();
    bool need_data = false;

    for (const auto & output_number : updated_outputs)
    {
        if (!skip_delayed || !port_pairs[output_number].is_delayed)
            need_data = processPair(port_pairs[output_number]) || need_data;
    }

    for (const auto & input_number : updated_inputs)
    {
        if (!skip_delayed || !port_pairs[input_number].is_delayed)
            need_data = processPair(port_pairs[input_number]) || need_data;
    }

    /// In case if main streams are finished at current iteration, start processing delayed streams.
    if (skip_delayed && (num_finished + num_delayed) >= port_pairs.size())
    {
        for (auto & pair : port_pairs)
            if (pair.is_delayed)
                need_data = processPair(pair) || need_data;
    }

    if (num_finished == port_pairs.size())
        return Status::Finished;

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}

}
