#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSource(const IProcessor & source)
{
    if (!source.getInputs().empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source for pipe shouldn't have any input, but {} has {} inputs",
            source.getName(),
            source.getInputs().size());

    if (source.getOutputs().size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source for pipe should have single output, but {} has {} outputs",
            source.getName(),
            source.getOutputs().size());
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

Pipe::Pipe() : processors(std::make_shared<Processors>())
{
}

Pipe::Pipe(ProcessorPtr source, OutputPort * output, OutputPort * totals, OutputPort * extremes)
    : processors(std::make_shared<Processors>())
{
    if (!source->getInputs().empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source for pipe shouldn't have any input, but {} has {} inputs",
            source->getName(),
            source->getInputs().size());

    if (!output)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create Pipe from source because specified output port is nullptr");

    if (output == totals || output == extremes || (totals && totals == extremes))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create Pipe from source because some of specified ports are the same");

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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create Pipe because specified {} port does not belong to source", name);
        };

        check_port_from_source(output, "output");
        check_port_from_source(totals, "totals");
        check_port_from_source(extremes, "extremes");

        if (num_specified_ports != outputs.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot create Pipe from source because it has {} output ports, but {} were specified",
                outputs.size(),
                num_specified_ports);
    }

    totals_port = totals;
    extremes_port = extremes;
    output_ports.push_back(output);
    processors->emplace_back(std::move(source));
    max_parallel_streams = 1;
}

Pipe::Pipe(ProcessorPtr source)
    : processors(std::make_shared<Processors>())
{
    checkSource(*source);

    if (collected_processors)
        collected_processors->emplace_back(source);

    output_ports.push_back(&source->getOutputs().front());
    header = output_ports.front()->getHeader();
    processors->emplace_back(std::move(source));
    max_parallel_streams = 1;
}

Pipe::Pipe(std::shared_ptr<Processors> processors_) : processors(std::move(processors_))
{
    /// Create hash table with processors.
    std::unordered_set<const IProcessor *> set;
    for (const auto & processor : *processors)
        set.emplace(processor.get());

    for (auto & processor : *processors)
    {
        for (const auto & port : processor->getInputs())
        {
            if (!port.isConnected())
                throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Cannot create Pipe because processor {} has disconnected input port",
                                processor->getName());

            const auto * connected_processor = &port.getOutputPort().getProcessor();
            if (!set.contains(connected_processor))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot create Pipe because processor {} has input port which is connected with unknown processor {}",
                    processor->getName(),
                    connected_processor->getName());
        }

        for (auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
            {
                output_ports.push_back(&port);
                continue;
            }

            const auto * connected_processor = &port.getInputPort().getProcessor();
            if (!set.contains(connected_processor))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot create Pipe because processor {} has output port which is connected with unknown processor {}",
                    processor->getName(),
                    connected_processor->getName());
        }
    }

    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create Pipe because processors don't have any disconnected output ports");

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipe");

    max_parallel_streams = output_ports.size();

    if (collected_processors)
        for (const auto & processor : *processors)
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

/// Calculate common header for pipes.
/// This function is needed only to remove ColumnConst from common header in case if some columns are const, and some not.
/// E.g. if the first header is `x, const y, const z` and the second is `const x, y, const z`, the common header will be `x, y, const z`.
static Block getCommonHeader(const Pipes & pipes)
{
    Block res;

    for (const auto & pipe : pipes)
    {
        if (const auto & header = pipe.getHeader())
        {
            res = header;
            break;
        }
    }

    for (const auto & pipe : pipes)
    {
        const auto & header = pipe.getHeader();
        for (size_t i = 0; i < res.columns(); ++i)
        {
            /// We do not check that headers are compatible here. Will do it later.

            if (i >= header.columns())
                break;

            auto & common = res.getByPosition(i).column;
            const auto & cur = header.getByPosition(i).column;

            /// Only remove const from common header if it is not const for current pipe.
            if (cur && common && !isColumnConst(*cur))
            {
                if (const auto * column_const = typeid_cast<const ColumnConst *>(common.get()))
                    common = column_const->getDataColumnPtr();
            }
        }
    }

    return res;
}

Pipe Pipe::unitePipes(Pipes pipes)
{
    return Pipe::unitePipes(std::move(pipes), nullptr, false);
}

Pipe Pipe::unitePipes(Pipes pipes, Processors * collected_processors, bool allow_empty_header)
{
    Pipe res;

    pipes = removeEmptyPipes(std::move(pipes));

    if (pipes.empty())
        return res;

    if (pipes.size() == 1)
        return std::move(pipes[0]);

    OutputPortRawPtrs totals;
    OutputPortRawPtrs extremes;
    res.collected_processors = collected_processors;
    res.header = getCommonHeader(pipes);

    for (auto & pipe : pipes)
    {
        if (!allow_empty_header || pipe.header)
            assertCompatibleHeader(pipe.header, res.header, "Pipe::unitePipes");

        res.processors->insert(res.processors->end(), pipe.processors->begin(), pipe.processors->end());
        res.output_ports.insert(res.output_ports.end(), pipe.output_ports.begin(), pipe.output_ports.end());

        res.max_parallel_streams += pipe.max_parallel_streams;

        if (pipe.totals_port)
            totals.emplace_back(pipe.totals_port);

        if (pipe.extremes_port)
            extremes.emplace_back(pipe.extremes_port);
    }

    size_t num_processors = res.processors->size();

    res.totals_port = uniteTotals(totals, res.header, *res.processors);
    res.extremes_port = uniteExtremes(extremes, res.header, *res.processors);

    if (res.collected_processors)
    {
        for (; num_processors < res.processors->size(); ++num_processors)
            res.collected_processors->emplace_back(res.processors->at(num_processors));
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
    processors->emplace_back(std::move(source));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addTotalsSource(ProcessorPtr source)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add totals source to empty Pipe");

    if (totals_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals source was already added to Pipe");

    checkSource(*source);
    const auto & source_header = output_ports.front()->getHeader();

    assertBlocksHaveEqualStructure(header, source_header, "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(source);

    totals_port = &source->getOutputs().front();
    processors->emplace_back(std::move(source));
}

void Pipe::addExtremesSource(ProcessorPtr source)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add extremes source to empty Pipe");

    if (extremes_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Extremes source was already added to Pipe");

    checkSource(*source);
    const auto & source_header = output_ports.front()->getHeader();

    assertBlocksHaveEqualStructure(header, source_header, "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(source);

    extremes_port = &source->getOutputs().front();
    processors->emplace_back(std::move(source));
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
    dropPort(totals_port, *processors, collected_processors);
}

void Pipe::dropExtremes()
{
    dropPort(extremes_port, *processors, collected_processors);
}

void Pipe::addTransform(ProcessorPtr transform)
{
    addTransform(std::move(transform), static_cast<OutputPort *>(nullptr), static_cast<OutputPort *>(nullptr));
}

void Pipe::addTransform(ProcessorPtr transform, OutputPort * totals, OutputPort * extremes)
{
    addTransform(std::move(transform),
        static_cast<InputPort *>(nullptr), static_cast<InputPort *>(nullptr),
        totals, extremes);
}

void Pipe::addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes)
{
    addTransform(std::move(transform),
        totals, extremes,
        static_cast<OutputPort *>(nullptr), static_cast<OutputPort *>(nullptr));
}

void Pipe::addTransform(
    ProcessorPtr transform,
    InputPort * totals_in, InputPort * extremes_in,
    OutputPort * totals_out, OutputPort * extremes_out)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform to empty Pipe");

    if (totals_in && !totals_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform consuming totals to Pipe because Pipe does not have totals");

    if (extremes_in && !extremes_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform consuming extremes to Pipe because Pipe does not have extremes");

    if (totals_out && !totals_in && totals_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform with totals to Pipe because it already has totals");

    if (extremes_out && !extremes_in && extremes_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform with extremes to Pipe because it already has extremes");

    auto & inputs = transform->getInputs();
    auto & outputs = transform->getOutputs();

    size_t expected_inputs = output_ports.size() + (totals_in ? 1 : 0) + (extremes_in ? 1 : 0);
    if (inputs.size() != expected_inputs)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipe because it has {} input ports, but {} expected",
            transform->getName(),
            inputs.size(),
            expected_inputs);

    if (outputs.size() <= (totals_out ? 1 : 0) + (extremes_out ? 1 : 0))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform {} to Pipes because it has no outputs",
                        transform->getName());

    bool found_totals_in = false;
    bool found_extremes_in = false;

    for (auto & input : inputs)
    {
        if (&input == totals_in)
            found_totals_in = true;
        else if (&input == extremes_in)
            found_extremes_in = true;
    }

    if (totals_in && !found_totals_in)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipes because specified totals port does not belong to it",
            transform->getName());

    if (extremes_in && !found_extremes_in)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipes because specified extremes port does not belong to it",
            transform->getName());

    bool found_totals_out = false;
    bool found_extremes_out = false;

    for (auto & output : outputs)
    {
        if (&output == totals_out)
            found_totals_out = true;
        else if (&output == extremes_out)
            found_extremes_out = true;
    }

    if (totals_out && !found_totals_out)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot add transform {} to Pipes because specified totals port does not belong to it",
                        transform->getName());

    if (extremes_out && !found_extremes_out)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot add transform {} to Pipes because specified extremes port does not belong to it",
                        transform->getName());

    if (totals_in)
    {
        connect(*totals_port, *totals_in);
        totals_port = nullptr;
    }
    if (extremes_in)
    {
        connect(*extremes_port, *extremes_in);
        extremes_port = nullptr;
    }

    totals_port = totals_out ? totals_out : totals_port;
    extremes_port = extremes_out ? extremes_out : extremes_port;

    size_t next_output = 0;
    for (auto & input : inputs)
    {
        if (&input != totals_in && &input != extremes_in)
        {
            connect(*output_ports[next_output], input);
            ++next_output;
        }
    }

    output_ports.clear();
    output_ports.reserve(outputs.size());
    for (auto & output : outputs)
    {
        if (&output != totals_out && &output != extremes_out)
            output_ports.emplace_back(&output);
    }

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipes");

    // Temporarily skip this check. TotalsHavingTransform may return finalized totals but not finalized data.
    // if (totals_port)
    //     assertBlocksHaveEqualStructure(header, totals_port->getHeader(), "Pipes");

    if (extremes_port)
        assertBlocksHaveEqualStructure(header, extremes_port->getHeader(), "Pipes");

    if (collected_processors)
        collected_processors->emplace_back(transform);

    processors->emplace_back(std::move(transform));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addSimpleTransform(const ProcessorGetterWithStreamKind & getter)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add simple transform to empty Pipe.");

    Block new_header;

    auto add_transform = [&](OutputPort *& port, StreamType stream_type)
    {
        if (!port)
            return;

        auto transform = getter(port->getHeader(), stream_type);

        if (transform)
        {
            if (transform->getInputs().size() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Processor for query pipeline transform should have single input, but {} has {} inputs",
                    transform->getName(),
                    transform->getInputs().size());

            if (transform->getOutputs().size() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Processor for query pipeline transform should have single output, but {} has {} outputs",
                    transform->getName(),
                    transform->getOutputs().size());
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

            processors->emplace_back(std::move(transform));
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

void Pipe::addChains(std::vector<Chain> chains)
{
    if (output_ports.size() != chains.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add chains to Pipe because number of output ports ({}) is not equal to the number of chains ({})",
            output_ports.size(),
            chains.size());

    dropTotals();
    dropExtremes();

    size_t max_parallel_streams_for_chains = 0;

    Block new_header;
    for (size_t i = 0; i < output_ports.size(); ++i)
    {
        max_parallel_streams_for_chains += std::max<size_t>(chains[i].getNumThreads(), 1);

        if (i == 0)
            new_header = chains[i].getOutputHeader();
        else
            assertBlocksHaveEqualStructure(new_header, chains[i].getOutputHeader(), "QueryPipeline");

        connect(*output_ports[i], chains[i].getInputPort());
        output_ports[i] = &chains[i].getOutputPort();

        auto added_processors = Chain::getProcessors(std::move(chains[i]));
        for (auto & transform : added_processors)
        {
            if (collected_processors)
                collected_processors->emplace_back(transform);

            processors->emplace_back(std::move(transform));
        }
    }

    header = std::move(new_header);
    max_parallel_streams = std::max(max_parallel_streams, max_parallel_streams_for_chains);
}

void Pipe::addSplitResizeTransform(size_t num_streams, size_t min_outstreams_per_resize_after_split, bool strict)
{
    OutputPortRawPtrs resize_output_ports(num_streams);

    size_t groups = std::min<size_t>(numOutputPorts(), num_streams / min_outstreams_per_resize_after_split);
    size_t instream_per_group = (numOutputPorts() + groups - 1) / groups;
    size_t groups_with_extra_instream = numOutputPorts() % groups;
    size_t outstreams_per_group = (num_streams + groups - 1) / groups;
    size_t groups_with_extra_outstream = num_streams % groups;

    chassert(groups > 1);

    for (size_t i = 0, next_input = 0, next_output = 0; i < groups; ++i)
    {
        ProcessorPtr resize;
        if (strict)
            resize = std::make_shared<StrictResizeProcessor>(getHeader(), instream_per_group, outstreams_per_group);
        else
            resize = std::make_shared<ResizeProcessor>(getHeader(), instream_per_group, outstreams_per_group);

        for (auto it = resize->getInputs().begin(); it != resize->getInputs().end(); ++it)
        {
            if (std::next(it) != resize->getInputs().end() || groups_with_extra_instream == 0 || i < groups_with_extra_instream)
            {
                connect(*output_ports[next_input], *it);
                ++next_input;
            }
            else
            {
                auto null_source = std::make_shared<NullSource>(getHeader());
                connect(null_source->getPort(), *it);
                processors->emplace_back(std::move(null_source));
            }
        }

        for (auto it = resize->getOutputs().begin(); it != resize->getOutputs().end(); ++it)
        {
            if (std::next(it) != resize->getOutputs().end() || groups_with_extra_outstream == 0 || i < groups_with_extra_outstream)
            {
                resize_output_ports[next_output] = &*it;
                ++next_output;
            }
            else
            {
                auto null_sink = std::make_shared<NullSink>(getHeader());
                connect(*it, null_sink->getPort());
                processors->emplace_back(std::move(null_sink));
            }
        }

        if (collected_processors)
            collected_processors->emplace_back(resize);
        processors->emplace_back(std::move(resize));
    }

    output_ports = std::move(resize_output_ports);

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipes");

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::resize(size_t num_streams, bool strict, UInt64 min_outstreams_per_resize_after_split)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot resize an empty Pipe");

    /// We need to not add the resize in case of 1-1 because in case
    /// it is not force resize and we have n outputs (look at the code above),
    /// and one of the outputs is dead, we can push all data to n-1 outputs,
    /// which doesn't make sense for 1-1 scenario
    if (numOutputPorts() == 1 && num_streams == 1)
        return;

    /// Performance bottleneck identified: Severe lock contention for ExecutingGraph::Node::status_mutex.
    /// Issues observed:
    /// 1. Increased latency of ExecutingGraph::updateNode, lengthening overall query latency.
    /// 2. Unnecessary CPU cycle consumption in native_queued_spin_lock_slowpath (kernel).
    /// 3. Decreased CPU utilization.
    ///
    /// Proposed solution: Split ResizeProcessor when multiple threads are allocated to execute a query.
    /// Benefits:
    /// 1. Mitigates lock contention.
    /// 2. Maintains ResizeProcessor's benefit of balancing data flow among multiple streams.
    ///
    /// Disable this optimization when min_outstreams_per_resize_after_split is 0
    if (output_ports.size() > 1 && min_outstreams_per_resize_after_split != 0 && num_streams / min_outstreams_per_resize_after_split > 1)
    {
        addSplitResizeTransform(num_streams, min_outstreams_per_resize_after_split, strict);
        return;
    }
    if (strict && num_streams == numOutputPorts())
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set sink to empty Pipe");

    auto add_transform = [&](OutputPort *& stream, Pipe::StreamType stream_type)
    {
        if (!stream)
            return;

        auto transform = getter(stream->getHeader(), stream_type);

        if (transform)
        {
            if (transform->getInputs().size() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Sink for query pipeline transform should have single input, but {} has {} inputs",
                    transform->getName(),
                    transform->getInputs().size());

            if (!transform->getOutputs().empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Sink for query pipeline transform should have no outputs, but {} has {} outputs",
                    transform->getName(),
                    transform->getOutputs().size());
        }

        if (!transform)
            transform = std::make_shared<NullSink>(stream->getHeader());

        connect(*stream, transform->getInputs().front());
        processors->emplace_back(std::move(transform));
    };

    for (auto & port : output_ports)
        add_transform(port, StreamType::Main);

    add_transform(totals_port, StreamType::Totals);
    add_transform(extremes_port, StreamType::Extremes);

    output_ports.clear();
    header.clear();
}

void Pipe::transform(const Transformer & transformer, bool check_ports)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot transform empty Pipe");

    auto new_processors = transformer(output_ports);

    /// Create hash table with new processors.
    std::unordered_set<const IProcessor *> set;
    for (const auto & processor : new_processors)
        set.emplace(processor.get());

    for (const auto & port : output_ports)
    {
        if (!check_ports)
            break;

        if (!port->isConnected())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Transformation of Pipe is not valid because output port ({})",
                port->getHeader().dumpStructure());

        set.emplace(&port->getProcessor());
    }

    output_ports.clear();

    for (const auto & processor : new_processors)
    {
        for (const auto & port : processor->getInputs())
        {
            if (!check_ports)
                break;

            if (!port.isConnected())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Transformation of Pipe is not valid because processor {} has not connected input port",
                    processor->getName());

            const auto * connected_processor = &port.getOutputPort().getProcessor();
            if (check_ports && !set.contains(connected_processor))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Transformation of Pipe is not valid because processor {} has input port which is connected with unknown processor {}",
                    processor->getName(),
                    connected_processor->getName());
        }

        for (auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
            {
                output_ports.push_back(&port);
                continue;
            }

            const auto * connected_processor = &port.getInputPort().getProcessor();
            if (check_ports && !set.contains(connected_processor))
                throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Transformation of Pipe is not valid because processor {} has output port which "
                                "is connected with unknown processor {}",
                                processor->getName(),
                                connected_processor->getName());
        }
    }

    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Transformation of Pipe is not valid because processors don't have any disconnected output ports");

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipe");

    if (totals_port)
        assertBlocksHaveEqualStructure(header, totals_port->getHeader(), "Pipes");

    if (extremes_port)
        assertBlocksHaveEqualStructure(header, extremes_port->getHeader(), "Pipes");

    if (collected_processors)
    {
        for (const auto & processor : new_processors)
            collected_processors->emplace_back(processor);
    }

    processors->insert(processors->end(), new_processors.begin(), new_processors.end());

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}


}
