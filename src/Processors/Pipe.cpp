#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/LimitTransform.h>
#include <Processors/NullSink.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSource(const IProcessor & source)
{
    if (!source.getInputs().empty())
        throw Exception("Source for pipe shouldn't have any input, but " + source.getName() + " has " +
                        toString(source.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (source.getOutputs().empty())
        throw Exception("Source for pipe should have single output, but it doesn't have any",
                        ErrorCodes::LOGICAL_ERROR);

    if (source.getOutputs().size() > 1)
        throw Exception("Source for pipe should have single or two outputs, but " + source.getName() + " has " +
                        toString(source.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

static OutputPort * uniteExtremes(const OutputPortRawPtrs & ports, const Block & header, Processors & processors)
{
    if (ports.empty())
        return nullptr;

    if (ports.size() == 1)
        return ports.front();

    /// Here we calculate extremes for extremes in case we unite several pipelines.
    /// Example: select number from numbers(2) union all select number from numbers(3)

    /// ->> Resize -> Extremes --(output port)----> Empty
    ///                        --(extremes port)--> ...

    auto resize = std::make_shared<ResizeProcessor>(header, ports.size(), 1);
    auto extremes = std::make_shared<ExtremesTransform>(header);
    auto sink = std::make_shared<EmptySink>(header);

    auto * extremes_port = &extremes->getExtremesPort();

    auto in = resize->getInputs().begin();
    for (const auto & port : ports)
        connect(*port, *(in++));

    connect(resize->getOutputs().front(), extremes->getInputPort());
    connect(extremes->getOutputPort(), sink->getPort());

    processors.emplace_back(std::move(resize));
    processors.emplace_back(std::move(extremes));
    processors.emplace_back(std::move(sink));

    return extremes_port;
}

static OutputPort * uniteTotals(const OutputPortRawPtrs & ports, const Block & header, Processors & processors)
{
    if (ports.empty())
        return nullptr;

    if (ports.size() == 1)
        return ports.front();

    /// Calculate totals from several streams.
    /// Take totals from first sources which has any, skip others.

    /// ->> Concat -> Limit

    auto concat = std::make_shared<ConcatProcessor>(header, ports.size());
    auto limit = std::make_shared<LimitTransform>(header, 1, 0);

    auto * totals_port = &limit->getOutputPort();

    auto in = concat->getInputs().begin();
    for (const auto & port : ports)
        connect(*port, *(in++));

    connect(concat->getOutputs().front(), limit->getInputPort());

    processors.emplace_back(std::move(concat));
    processors.emplace_back(std::move(limit));

    return totals_port;
}

Pipe::Holder & Pipe::Holder::operator=(Holder && rhs)
{
    table_locks.insert(table_locks.end(), rhs.table_locks.begin(), rhs.table_locks.end());
    storage_holders.insert(storage_holders.end(), rhs.storage_holders.begin(), rhs.storage_holders.end());
    interpreter_context.insert(interpreter_context.end(),
                               rhs.interpreter_context.begin(), rhs.interpreter_context.end());
    for (auto & plan : rhs.query_plans)
        query_plans.emplace_back(std::move(plan));

    return *this;
}

Pipe::Pipe(ProcessorPtr source, OutputPort * output, OutputPort * totals, OutputPort * extremes)
{
    if (!source->getInputs().empty())
        throw Exception("Source for pipe shouldn't have any input, but " + source->getName() + " has " +
                        toString(source->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (!output)
        throw Exception("Cannot create Pipe from source because specified output port is nullptr",
                        ErrorCodes::LOGICAL_ERROR);

    if (output == totals || output == extremes || (totals && totals == extremes))
        throw Exception("Cannot create Pipe from source because some of specified ports are the same",
                        ErrorCodes::LOGICAL_ERROR);

    header = output->getHeader();

    /// Check that ports belong to source and all ports from source were specified.
    {
        auto & outputs = source->getOutputs();
        size_t num_specified_ports = 0;

        auto check_port_from_source = [&](OutputPort * port, std::string name)
        {
            if (!port)
                return;

            assertBlocksHaveEqualStructure(header, port->getHeader(), name);

            ++num_specified_ports;

            auto it = std::find_if(outputs.begin(), outputs.end(), [port](const OutputPort & p) { return &p == port; });
            if (it == outputs.end())
                throw Exception("Cannot create Pipe because specified " + name + " port does not belong to source",
                                ErrorCodes::LOGICAL_ERROR);
        };

        check_port_from_source(output, "output");
        check_port_from_source(totals, "totals");
        check_port_from_source(extremes, "extremes");

        if (num_specified_ports != outputs.size())
            throw Exception("Cannot create Pipe from source because it has " + std::to_string(outputs.size()) +
                            " output ports, but " + std::to_string(num_specified_ports) + " were specified",
                            ErrorCodes::LOGICAL_ERROR);
    }

    totals_port = totals;
    extremes_port = extremes;
    output_ports.push_back(output);
    processors.emplace_back(std::move(source));
    max_parallel_streams = 1;
}

Pipe::Pipe(ProcessorPtr source)
{
    if (auto * source_from_input_stream = typeid_cast<SourceFromInputStream *>(source.get()))
    {
        /// Special case for SourceFromInputStream. Will remove it later.
        totals_port = source_from_input_stream->getTotalsPort();
        extremes_port = source_from_input_stream->getExtremesPort();
    }
    else if (source->getOutputs().size() != 1)
        checkSource(*source);

    if (collected_processors)
        collected_processors->emplace_back(source);

    output_ports.push_back(&source->getOutputs().front());
    header = output_ports.front()->getHeader();
    processors.emplace_back(std::move(source));
    max_parallel_streams = 1;
}

Pipe::Pipe(Processors processors_) : processors(std::move(processors_))
{
    /// Create hash table with processors.
    std::unordered_set<const IProcessor *> set;
    for (const auto & processor : processors)
        set.emplace(processor.get());

    for (auto & processor : processors)
    {
        for (const auto & port : processor->getInputs())
        {
            if (!port.isConnected())
                throw Exception("Cannot create Pipe because processor " + processor->getName() +
                                " has not connected input port", ErrorCodes::LOGICAL_ERROR);

            const auto * connected_processor = &port.getOutputPort().getProcessor();
            if (set.count(connected_processor) == 0)
                throw Exception("Cannot create Pipe because processor " + processor->getName() +
                                " has input port which is connected with unknown processor " +
                                connected_processor->getName(), ErrorCodes::LOGICAL_ERROR);
        }

        for (auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
            {
                output_ports.push_back(&port);
                continue;
            }

            const auto * connected_processor = &port.getInputPort().getProcessor();
            if (set.count(connected_processor) == 0)
                throw Exception("Cannot create Pipe because processor " + processor->getName() +
                                " has output port which is connected with unknown processor " +
                                connected_processor->getName(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (output_ports.empty())
        throw Exception("Cannot create Pipe because processors don't have any not-connected output ports",
                        ErrorCodes::LOGICAL_ERROR);

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipe");

    max_parallel_streams = output_ports.size();

    if (collected_processors)
        for (const auto & processor : processors)
            collected_processors->emplace_back(processor);
}

static Pipes removeEmptyPipes(Pipes pipes)
{
    Pipes res;
    res.reserve(pipes.size());

    for (auto & pipe : pipes)
    {
        if (!pipe.empty())
            res.emplace_back(std::move(pipe));
    }

    return res;
}

Pipe Pipe::unitePipes(Pipes pipes)
{
    return Pipe::unitePipes(std::move(pipes), nullptr);
}

Pipe Pipe::unitePipes(Pipes pipes, Processors * collected_processors)
{
    Pipe res;

    for (auto & pipe : pipes)
        res.holder = std::move(pipe.holder); /// see move assignment for Pipe::Holder.

    pipes = removeEmptyPipes(std::move(pipes));

    if (pipes.empty())
        return res;

    if (pipes.size() == 1)
    {
        pipes[0].holder = std::move(res.holder);
        return std::move(pipes[0]);
    }

    OutputPortRawPtrs totals;
    OutputPortRawPtrs extremes;
    res.header = pipes.front().header;
    res.collected_processors = collected_processors;

    for (auto & pipe : pipes)
    {
        assertBlocksHaveEqualStructure(res.header, pipe.header, "Pipe::unitePipes");
        res.processors.insert(res.processors.end(), pipe.processors.begin(), pipe.processors.end());
        res.output_ports.insert(res.output_ports.end(), pipe.output_ports.begin(), pipe.output_ports.end());

        res.max_parallel_streams += pipe.max_parallel_streams;

        if (pipe.totals_port)
            totals.emplace_back(pipe.totals_port);

        if (pipe.extremes_port)
            extremes.emplace_back(pipe.extremes_port);
    }

    size_t num_processors = res.processors.size();

    res.totals_port = uniteTotals(totals, res.header, res.processors);
    res.extremes_port = uniteExtremes(extremes, res.header, res.processors);

    if (res.collected_processors)
    {
        for (; num_processors < res.processors.size(); ++num_processors)
            res.collected_processors->emplace_back(res.processors[num_processors]);
    }

    return res;
}

void Pipe::addSource(ProcessorPtr source)
{
    checkSource(*source);
    const auto & source_header = source->getOutputs().front().getHeader();

    if (output_ports.empty())
        header = source_header;
    else
        assertBlocksHaveEqualStructure(header, source_header, "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(source);

    output_ports.push_back(&source->getOutputs().front());
    processors.emplace_back(std::move(source));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addTotalsSource(ProcessorPtr source)
{
    if (output_ports.empty())
        throw Exception("Cannot add totals source to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    if (totals_port)
        throw Exception("Totals source was already added to Pipe.", ErrorCodes::LOGICAL_ERROR);

    checkSource(*source);
    const auto & source_header = output_ports.front()->getHeader();

    assertBlocksHaveEqualStructure(header, source_header, "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(source);

    totals_port = &source->getOutputs().front();
    processors.emplace_back(std::move(source));
}

void Pipe::addExtremesSource(ProcessorPtr source)
{
    if (output_ports.empty())
        throw Exception("Cannot add extremes source to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    if (extremes_port)
        throw Exception("Extremes source was already added to Pipe.", ErrorCodes::LOGICAL_ERROR);

    checkSource(*source);
    const auto & source_header = output_ports.front()->getHeader();

    assertBlocksHaveEqualStructure(header, source_header, "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(source);

    extremes_port = &source->getOutputs().front();
    processors.emplace_back(std::move(source));
}

static void dropPort(OutputPort *& port, Processors & processors, Processors * collected_processors)
{
    if (port == nullptr)
        return;

    auto null_sink = std::make_shared<NullSink>(port->getHeader());
    connect(*port, null_sink->getPort());

    if (collected_processors)
        collected_processors->emplace_back(null_sink);

    processors.emplace_back(std::move(null_sink));
    port = nullptr;
}

void Pipe::dropTotals()
{
    dropPort(totals_port, processors, collected_processors);
}

void Pipe::dropExtremes()
{
    dropPort(extremes_port, processors, collected_processors);
}

void Pipe::addTransform(ProcessorPtr transform)
{
    addTransform(std::move(transform), static_cast<OutputPort *>(nullptr), static_cast<OutputPort *>(nullptr));
}

void Pipe::addTransform(ProcessorPtr transform, OutputPort * totals, OutputPort * extremes)
{
    if (output_ports.empty())
        throw Exception("Cannot add transform to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    auto & inputs = transform->getInputs();
    if (inputs.size() != output_ports.size())
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "Processor has " + std::to_string(inputs.size()) + " input ports, "
                        "but " + std::to_string(output_ports.size()) + " expected", ErrorCodes::LOGICAL_ERROR);

    if (totals && totals_port)
        throw Exception("Cannot add transform with totals to Pipe because it already has totals.",
                        ErrorCodes::LOGICAL_ERROR);

    if (extremes && extremes_port)
        throw Exception("Cannot add transform with extremes to Pipe because it already has extremes.",
                        ErrorCodes::LOGICAL_ERROR);

    if (totals)
        totals_port = totals;
    if (extremes)
        extremes_port = extremes;

    size_t next_output = 0;
    for (auto & input : inputs)
    {
        connect(*output_ports[next_output], input);
        ++next_output;
    }

    auto & outputs = transform->getOutputs();

    output_ports.clear();
    output_ports.reserve(outputs.size());

    bool found_totals = false;
    bool found_extremes = false;

    for (auto & output : outputs)
    {
        if (&output == totals)
            found_totals = true;
        else if (&output == extremes)
            found_extremes = true;
        else
            output_ports.emplace_back(&output);
    }

    if (totals && !found_totals)
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "specified totals port does not belong to it", ErrorCodes::LOGICAL_ERROR);

    if (extremes && !found_extremes)
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "specified extremes port does not belong to it", ErrorCodes::LOGICAL_ERROR);

    if (output_ports.empty())
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because it has no outputs",
                        ErrorCodes::LOGICAL_ERROR);

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipes");

    // Temporarily skip this check. TotaslHavingTransform may return finalized totals but not finalized data.
    // if (totals_port)
    //     assertBlocksHaveEqualStructure(header, totals_port->getHeader(), "Pipes");

    if (extremes_port)
        assertBlocksHaveEqualStructure(header, extremes_port->getHeader(), "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(transform);

    processors.emplace_back(std::move(transform));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes)
{
    if (output_ports.empty())
        throw Exception("Cannot add transform to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    auto & inputs = transform->getInputs();
    size_t expected_inputs = output_ports.size() + (totals ? 1 : 0) + (extremes ? 1 : 0);
    if (inputs.size() != expected_inputs)
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "Processor has " + std::to_string(inputs.size()) + " input ports, "
                        "but " + std::to_string(expected_inputs) + " expected", ErrorCodes::LOGICAL_ERROR);

    if (totals && !totals_port)
        throw Exception("Cannot add transform consuming totals to Pipe because Pipe does not have totals.",
                        ErrorCodes::LOGICAL_ERROR);

    if (extremes && !extremes_port)
        throw Exception("Cannot add transform consuming extremes to Pipe because it already has extremes.",
                        ErrorCodes::LOGICAL_ERROR);

    if (totals)
    {
        connect(*totals_port, *totals);
        totals_port = nullptr;
    }
    if (extremes)
    {
        connect(*extremes_port, *extremes);
        extremes_port = nullptr;
    }

    bool found_totals = false;
    bool found_extremes = false;

    size_t next_output = 0;
    for (auto & input : inputs)
    {
        if (&input == totals)
            found_totals = true;
        else if (&input == extremes)
            found_extremes = true;
        else
        {
            connect(*output_ports[next_output], input);
            ++next_output;
        }
    }

    if (totals && !found_totals)
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "specified totals port does not belong to it", ErrorCodes::LOGICAL_ERROR);

    if (extremes && !found_extremes)
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "specified extremes port does not belong to it", ErrorCodes::LOGICAL_ERROR);

    auto & outputs = transform->getOutputs();
    if (outputs.empty())
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because it has no outputs",
                        ErrorCodes::LOGICAL_ERROR);

    output_ports.clear();
    output_ports.reserve(outputs.size());

    for (auto & output : outputs)
        output_ports.emplace_back(&output);

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipes");

    if (totals_port)
        assertBlocksHaveEqualStructure(header, totals_port->getHeader(), "Pipes");

    if (extremes_port)
        assertBlocksHaveEqualStructure(header, extremes_port->getHeader(), "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(transform);

    processors.emplace_back(std::move(transform));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addSimpleTransform(const ProcessorGetterWithStreamKind & getter)
{
    if (output_ports.empty())
        throw Exception("Cannot add simple transform to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    Block new_header;

    auto add_transform = [&](OutputPort *& port, StreamType stream_type)
    {
        if (!port)
            return;

        auto transform = getter(port->getHeader(), stream_type);

        if (transform)
        {
            if (transform->getInputs().size() != 1)
                throw Exception("Processor for query pipeline transform should have single input, "
                                "but " + transform->getName() + " has " +
                                toString(transform->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

            if (transform->getOutputs().size() != 1)
                throw Exception("Processor for query pipeline transform should have single output, "
                                "but " + transform->getName() + " has " +
                                toString(transform->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
        }

        const auto & out_header = transform ? transform->getOutputs().front().getHeader()
                                            : port->getHeader();

        if (new_header)
            assertBlocksHaveEqualStructure(new_header, out_header, "QueryPipeline");
        else
            new_header = out_header;

        if (transform)
        {
            connect(*port, transform->getInputs().front());
            port = &transform->getOutputs().front();

            if (collected_processors)
                collected_processors->emplace_back(transform);

            processors.emplace_back(std::move(transform));
        }
    };

    for (auto & port : output_ports)
        add_transform(port, StreamType::Main);

    add_transform(totals_port, StreamType::Totals);
    add_transform(extremes_port, StreamType::Extremes);

    header = std::move(new_header);
}

void Pipe::addSimpleTransform(const ProcessorGetter & getter)
{
    addSimpleTransform([&](const Block & stream_header, StreamType) { return getter(stream_header); });
}

void Pipe::resize(size_t num_streams, bool force, bool strict)
{
    if (output_ports.empty())
        throw Exception("Cannot resize an empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    if (!force && num_streams == numOutputPorts())
        return;

    ProcessorPtr resize;

    if (strict)
        resize = std::make_shared<StrictResizeProcessor>(getHeader(), numOutputPorts(), num_streams);
    else
        resize = std::make_shared<ResizeProcessor>(getHeader(), numOutputPorts(), num_streams);

    addTransform(std::move(resize));
}

void Pipe::setSinks(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    if (output_ports.empty())
        throw Exception("Cannot set sink to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    auto add_transform = [&](OutputPort *& stream, Pipe::StreamType stream_type)
    {
        if (!stream)
            return;

        auto transform = getter(stream->getHeader(), stream_type);

        if (transform)
        {
            if (transform->getInputs().size() != 1)
                throw Exception("Sink for query pipeline transform should have single input, "
                                "but " + transform->getName() + " has " +
                                toString(transform->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

            if (!transform->getOutputs().empty())
                throw Exception("Sink for query pipeline transform should have no outputs, "
                                "but " + transform->getName() + " has " +
                                toString(transform->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
        }

        if (!transform)
            transform = std::make_shared<NullSink>(stream->getHeader());

        connect(*stream, transform->getInputs().front());
        processors.emplace_back(std::move(transform));
    };

    for (auto & port : output_ports)
        add_transform(port, StreamType::Main);

    add_transform(totals_port, StreamType::Totals);
    add_transform(extremes_port, StreamType::Extremes);

    output_ports.clear();
    header.clear();
}

void Pipe::setOutputFormat(ProcessorPtr output)
{
    if (output_ports.empty())
        throw Exception("Cannot set output format to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    if (output_ports.size() != 1)
        throw Exception("Cannot set output format to Pipe because single output port is expected, "
                        "but it has " + std::to_string(output_ports.size()) + " ports", ErrorCodes::LOGICAL_ERROR);

    auto * format = dynamic_cast<IOutputFormat * >(output.get());

    if (!format)
        throw Exception("IOutputFormat processor expected for QueryPipeline::setOutputFormat.",
                        ErrorCodes::LOGICAL_ERROR);

    auto & main = format->getPort(IOutputFormat::PortKind::Main);
    auto & totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals_port)
        addTotalsSource(std::make_shared<NullSource>(totals.getHeader()));

    if (!extremes_port)
        addExtremesSource(std::make_shared<NullSource>(extremes.getHeader()));

    if (collected_processors)
        collected_processors->emplace_back(output);

    processors.emplace_back(std::move(output));

    connect(*output_ports.front(), main);
    connect(*totals_port, totals);
    connect(*extremes_port, extremes);

    output_ports.clear();
    header.clear();
}

void Pipe::transform(const Transformer & transformer)
{
    if (output_ports.empty())
        throw Exception("Cannot transform empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    auto new_processors = transformer(output_ports);

    /// Create hash table with new processors.
    std::unordered_set<const IProcessor *> set;
    for (const auto & processor : new_processors)
        set.emplace(processor.get());

    for (const auto & port : output_ports)
    {
        if (!port->isConnected())
            throw Exception("Transformation of Pipe is not valid because output port (" +
                            port->getHeader().dumpStructure() + ") is not connected", ErrorCodes::LOGICAL_ERROR);

        set.emplace(&port->getProcessor());
    }

    output_ports.clear();

    for (const auto & processor : new_processors)
    {
        for (const auto & port : processor->getInputs())
        {
            if (!port.isConnected())
                throw Exception("Transformation of Pipe is not valid because processor " + processor->getName() +
                                " has not connected input port", ErrorCodes::LOGICAL_ERROR);

            const auto * connected_processor = &port.getOutputPort().getProcessor();
            if (set.count(connected_processor) == 0)
                throw Exception("Transformation of Pipe is not valid because processor " + processor->getName() +
                                " has input port which is connected with unknown processor " +
                                connected_processor->getName(), ErrorCodes::LOGICAL_ERROR);
        }

        for (auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
            {
                output_ports.push_back(&port);
                continue;
            }

            const auto * connected_processor = &port.getInputPort().getProcessor();
            if (set.count(connected_processor) == 0)
                throw Exception("Transformation of Pipe is not valid because processor " + processor->getName() +
                                " has output port which is connected with unknown processor " +
                                connected_processor->getName(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (output_ports.empty())
        throw Exception("Transformation of Pipe is not valid because processors don't have any "
                        "not-connected output ports", ErrorCodes::LOGICAL_ERROR);

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipe");

    if (totals_port)
        assertBlocksHaveEqualStructure(header, totals_port->getHeader(), "Pipes");

    if (extremes_port)
        assertBlocksHaveEqualStructure(header, extremes_port->getHeader(), "Pipes");

    if (collected_processors)
    {
        for (const auto & processor : processors)
            collected_processors->emplace_back(processor);
    }

    processors.insert(processors.end(), new_processors.begin(), new_processors.end());

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::setLimits(const StreamLocalLimits & limits)
{
    for (auto & processor : processors)
    {
        if (auto * source_with_progress = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source_with_progress->setLimits(limits);
    }
}

void Pipe::setLeafLimits(const SizeLimits & leaf_limits)
{
    for (auto & processor : processors)
    {
        if (auto * source_with_progress = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source_with_progress->setLeafLimits(leaf_limits);
    }
}

void Pipe::setQuota(const std::shared_ptr<const EnabledQuota> & quota)
{
    for (auto & processor : processors)
    {
        if (auto * source_with_progress = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source_with_progress->setQuota(quota);
    }
}

}
