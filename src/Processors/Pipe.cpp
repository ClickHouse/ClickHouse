#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/LimitTransform.h>
#include <Processors/NullSink.h>
#include <Processors/Transforms/ExtremesTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSingleInput(const IProcessor & transform)
{
    if (transform.getInputs().size() != 1)
        throw Exception("Processor for pipe should have single input, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);
}

static void checkMultipleInputs(const IProcessor & transform, size_t num_inputs)
{
    if (transform.getInputs().size() != num_inputs)
        throw Exception("Processor for pipe should have " + toString(num_inputs) + " inputs, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSingleOutput(const IProcessor & transform)
{
    if (transform.getOutputs().size() != 1)
        throw Exception("Processor for pipe should have single output, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSimpleTransform(const IProcessor & transform)
{
    checkSingleInput(transform);
    checkSingleOutput(transform);
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

    /// Calculate totals fro several streams.
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
    pipes = removeEmptyPipes(std::move(pipes));

    if (pipes.empty())
        return {};

    if (pipes.size() == 1)
        return std::move(pipes[0]);

    Pipe res;
    OutputPortRawPtrs totals;
    OutputPortRawPtrs extremes;
    res.header = pipes.front().header;

    for (auto & pipe : pipes)
    {
        assertBlocksHaveEqualStructure(res.header, pipe.header, "Pipe::unitePipes");
        res.processors.insert(res.processors.end(), pipe.processors.begin(), pipe.processors.end());
        res.output_ports.insert(res.output_ports.end(), pipe.output_ports.begin(), pipe.output_ports.end());
        res.table_locks.insert(res.table_locks.end(), pipe.table_locks.begin(), pipe.table_locks.end());
        res.storage_holders.insert(res.storage_holders.end(), pipe.storage_holders.begin(), pipe.storage_holders.end());
        res.interpreter_context.insert(res.interpreter_context.end(),
                                       pipe.interpreter_context.begin(), pipe.interpreter_context.end());

        res.max_parallel_streams += pipe.max_parallel_streams;

        if (pipe.totals_port)
            totals.emplace_back(pipe.totals_port);

        if (pipe.extremes_port)
            extremes.emplace_back(pipe.extremes_port);
    }

    res.totals_port = uniteTotals(totals, res.header, res.processors);
    res.extremes_port = uniteExtremes(extremes, res.header, res.processors);
}

//void Pipe::addPipes(Pipe pipes)
//{
//    if (processors.empty())
//    {
//        *this = std::move(pipes);
//        return;
//    }
//
//    if (pipes.processors.empty())
//        return;
//
//    assertBlocksHaveEqualStructure(header, pipes.header, "Pipe");
//
//    max_parallel_streams += pipes.max_parallel_streams;
//    processors.insert(processors.end(), pipes.processors.begin(), pipes.processors.end());
//
//    OutputPortRawPtrs totals;
//    if (totals_port)
//        totals.emplace_back(totals_port);
//    if (pipes.totals_port)
//        totals.emplace_back(pipes.totals_port);
//    if (!totals.empty())
//        totals_port = uniteTotals(totals, header, processors);
//
//    OutputPortRawPtrs extremes;
//    if (extremes_port)
//        extremes.emplace_back(extremes_port);
//    if (pipes.extremes_port)
//        extremes.emplace_back(pipes.extremes_port);
//    if (!extremes.empty())
//        extremes_port = uniteExtremes(extremes, header, processors);
//}

//void Pipe::addSource(ProcessorPtr source)
//{
//    checkSource(*source);
//    const auto & source_header = output_ports.front()->getHeader();
//
//    assertBlocksHaveEqualStructure(header, source_header, "Pipes"); !!!!
//
//    output_ports.push_back(&source->getOutputs().front());
//    processors.emplace_back(std::move(source));
//
//    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
//}

void Pipe::addTotalsSource(ProcessorPtr source)
{
    if (output_ports.empty())
        throw Exception("Cannot add totals source to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    if (totals_port)
        throw Exception("Totals source was already added to Pipe.", ErrorCodes::LOGICAL_ERROR);

    checkSource(*source);
    const auto & source_header = output_ports.front()->getHeader();

    assertBlocksHaveEqualStructure(header, source_header, "Pipes");

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

    extremes_port = &source->getOutputs().front();
    processors.emplace_back(std::move(source));
}

void Pipe::addTransform(ProcessorPtr transform)
{
    if (output_ports.empty())
        throw Exception("Cannot add transform to empty Pipe.", ErrorCodes::LOGICAL_ERROR);

    auto & inputs = transform->getInputs();
    if (inputs.size() != output_ports.size())
        throw Exception("Cannot add transform " + transform->getName() + " to Pipes because "
                        "Processor has " + std::to_string(inputs.size()) + " input ports, "
                        "but " + std::to_string(output_ports.size()) + " expected", ErrorCodes::LOGICAL_ERROR);

    size_t next_output = 0;
    for (auto & input : inputs)
    {
        connect(*output_ports[next_output], input);
        ++next_output;
    }

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

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addSimpleTransform(const ProcessorGetter & getter)
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
            processors.emplace_back(std::move(transform));
        }
    };

    for (auto & port : output_ports)
        add_transform(port, StreamType::Main);

    add_transform(totals_port, StreamType::Totals);
    add_transform(extremes_port, StreamType::Extremes);

    header = std::move(new_header);
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

    OutputPortRawPtrs new_output_ports;
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
                new_output_ports.push_back(&port);
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

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

/*
Pipe::Pipe(ProcessorPtr source)
{
    if (auto * source_from_input_stream = typeid_cast<SourceFromInputStream *>(source.get()))
    {
        totals = source_from_input_stream->getTotalsPort();
        extremes = source_from_input_stream->getExtremesPort();
    }
    else if (source->getOutputs().size() != 1)
        checkSource(*source);

    output_port = &source->getOutputs().front();

    processors.emplace_back(std::move(source));
    max_parallel_streams = 1;
}

Pipe::Pipe(Processors processors_, OutputPort * output_port_, OutputPort * totals_, OutputPort * extremes_)
    : processors(std::move(processors_)), output_port(output_port_), totals(totals_), extremes(extremes_)
{
}

Pipe::Pipe(Pipes && pipes, ProcessorPtr transform)
{
    checkSingleOutput(*transform);
    checkMultipleInputs(*transform, pipes.size());

    auto it = transform->getInputs().begin();

    for (auto & pipe : pipes)
    {
        connect(*pipe.output_port, *it);
        ++it;

        max_parallel_streams += pipe.max_parallel_streams;
        processors.insert(processors.end(), pipe.processors.begin(), pipe.processors.end());

        std::move(pipe.table_locks.begin(), pipe.table_locks.end(), std::back_inserter(table_locks));
        std::move(pipe.interpreter_context.begin(), pipe.interpreter_context.end(), std::back_inserter(interpreter_context));
        std::move(pipe.storage_holders.begin(), pipe.storage_holders.end(), std::back_inserter(storage_holders));
    }

    output_port = &transform->getOutputs().front();
    processors.emplace_back(std::move(transform));
}

Pipe::Pipe(OutputPort * port) : output_port(port)
{
}

void Pipe::addProcessors(const Processors & processors_)
{
    processors.insert(processors.end(), processors_.begin(), processors_.end());
}

void Pipe::addSimpleTransform(ProcessorPtr transform)
{
    checkSimpleTransform(*transform);
    connect(*output_port, transform->getInputs().front());
    output_port = &transform->getOutputs().front();
    processors.emplace_back(std::move(transform));
}

void Pipe::setLimits(const ISourceWithProgress::LocalLimits & limits)
{
    for (auto & processor : processors)
    {
        if (auto * source_with_progress = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source_with_progress->setLimits(limits);
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

void Pipe::pinSources(size_t executor_number)
{
    for (auto & processor : processors)
    {
        if (auto * source = dynamic_cast<ISource *>(processor.get()))
            source->setStream(executor_number);
    }
}

void Pipe::enableQuota()
{
    for (auto & processor : processors)
    {
        if (auto * source = dynamic_cast<ISource *>(processor.get()))
            source->enableQuota();
    }
}
*/
}
