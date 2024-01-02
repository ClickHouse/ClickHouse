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
#include <QueryPipeline/ReadProgressCallback.h>
#include <Columns/ColumnConst.h>
#include <Common/logger_useful.h>

#include <QueryPipeline/printPipeline.h>

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

    output_ports.push_back(&source->getOutputs().front());
    header = output_ports.front()->getHeader();
    addProcessor(std::move(source));
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
        if (res.isPartialResultActive() && pipe.isPartialResultActive())
        {
            res.partial_result_ports.insert(res.partial_result_ports.end(), pipe.partial_result_ports.begin(), pipe.partial_result_ports.end());
        }
        else
        {
            if (pipe.isPartialResultActive())
                pipe.dropPartialResult();
            if (res.isPartialResultActive())
                res.dropPartialResult();
        }

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

    output_ports.push_back(&source->getOutputs().front());
    if (isPartialResultActive())
        partial_result_ports.push_back(nullptr);

    addProcessor(std::move(source));

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

    totals_port = &source->getOutputs().front();

    addProcessor(std::move(source));
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

    extremes_port = &source->getOutputs().front();

    addProcessor(std::move(source));
}

void Pipe::activatePartialResult(UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_)
{
    if (is_partial_result_active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partial result for Pipe should be initialized only once");

    is_partial_result_active = true;
    partial_result_limit = partial_result_limit_;
    partial_result_duration_ms = partial_result_duration_ms_;
    partial_result_ports.assign(output_ports.size(), nullptr);
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

void Pipe::dropPartialResult()
{
    for (auto & port : partial_result_ports)
        dropPort(port, *processors, collected_processors);

    is_partial_result_active = false;
    partial_result_ports.clear();
}

void Pipe::addTransform(ProcessorPtr transform)
{
    addTransform(std::move(transform), static_cast<OutputPort *>(nullptr), static_cast<OutputPort *>(nullptr));
}

void Pipe::addTransform(ProcessorPtr transform, OutputPort * totals, OutputPort * extremes)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform to empty Pipe");

    auto & inputs = transform->getInputs();
    if (inputs.size() != output_ports.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipe because it has {} input ports, but {} expected",
            transform->getName(),
            inputs.size(),
            output_ports.size());

    if (totals && totals_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform with totals to Pipe because it already has totals");

    if (extremes && extremes_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform with extremes to Pipe because it already has extremes");

    if (totals)
        totals_port = totals;
    if (extremes)
        extremes_port = extremes;

    addPartialResultTransform(transform);

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
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot add transform {} to Pipes because specified totals port does not belong to it",
                        transform->getName());

    if (extremes && !found_extremes)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot add transform {} to Pipes because specified extremes port does not belong to it",
                        transform->getName());

    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform {} to Pipes because it has no outputs",
                        transform->getName());

    header = output_ports.front()->getHeader();
    for (size_t i = 1; i < output_ports.size(); ++i)
        assertBlocksHaveEqualStructure(header, output_ports[i]->getHeader(), "Pipes");

    // Temporarily skip this check. TotalsHavingTransform may return finalized totals but not finalized data.
    // if (totals_port)
    //     assertBlocksHaveEqualStructure(header, totals_port->getHeader(), "Pipes");

    if (extremes_port)
        assertBlocksHaveEqualStructure(header, extremes_port->getHeader(), "Pipes");

    addProcessor(std::move(transform));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform to empty Pipe");

    auto & inputs = transform->getInputs();
    size_t expected_inputs = output_ports.size() + (totals ? 1 : 0) + (extremes ? 1 : 0);
    if (inputs.size() != expected_inputs)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipe because it has {} input ports, but {} expected",
            transform->getName(),
            inputs.size(),
            expected_inputs);

    if (totals && !totals_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform consuming totals to Pipe because Pipe does not have totals");

    if (extremes && !extremes_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform consuming extremes to Pipe because Pipe does not have extremes");

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

    addPartialResultTransform(transform);

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
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipes because specified totals port does not belong to it",
            transform->getName());

    if (extremes && !found_extremes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add transform {} to Pipes because specified extremes port does not belong to it",
            transform->getName());

    auto & outputs = transform->getOutputs();
    if (outputs.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add transform {} to Pipes because it has no outputs", transform->getName());

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

    addProcessor(std::move(transform));

    max_parallel_streams = std::max<size_t>(max_parallel_streams, output_ports.size());
}

void Pipe::addPartialResultSimpleTransform(const ProcessorPtr & transform, size_t partial_result_port_id)
{
    if (isPartialResultActive())
    {
        auto & partial_result_port = partial_result_ports[partial_result_port_id];
        auto partial_result_status = transform->getPartialResultProcessorSupportStatus();

        if (partial_result_status == IProcessor::PartialResultStatus::NotSupported)
            dropPort(partial_result_port, *processors, collected_processors);

        if (partial_result_status != IProcessor::PartialResultStatus::FullSupported)
            return;

        auto partial_result_transform = IProcessor::getPartialResultProcessorPtr(transform, partial_result_limit, partial_result_duration_ms);

        connectPartialResultPort(partial_result_port, partial_result_transform->getInputs().front());

        partial_result_port = &partial_result_transform->getOutputs().front();

        addProcessor(std::move(partial_result_transform));
    }
}

void Pipe::addPartialResultTransform(const ProcessorPtr & transform)
{
    if (isPartialResultActive())
    {
        size_t new_outputs_size = 0;
        for (const auto & output : transform->getOutputs())
        {
            /// We do not use totals_port and extremes_port in partial result
            if ((totals_port && totals_port == &output) || (extremes_port && extremes_port == &output))
                continue;
            ++new_outputs_size;
        }

        auto partial_result_status = transform->getPartialResultProcessorSupportStatus();

        if (partial_result_status == IProcessor::PartialResultStatus::SkipSupported && new_outputs_size != partial_result_ports.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot skip transform {} in the partial result part of the Pipe because it has {} output ports, but the partial result part expects {} output ports",
                transform->getName(),
                new_outputs_size,
                partial_result_ports.size());

        if (partial_result_status == IProcessor::PartialResultStatus::NotSupported)
        {
            for (auto & partial_result_port : partial_result_ports)
                dropPort(partial_result_port, *processors, collected_processors);

            partial_result_ports.assign(new_outputs_size, nullptr);
            return;
        }

        if (partial_result_status != IProcessor::PartialResultStatus::FullSupported)
            return;

        auto partial_result_transform = IProcessor::getPartialResultProcessorPtr(transform, partial_result_limit, partial_result_duration_ms);
        auto & inputs = partial_result_transform->getInputs();

        if (inputs.size() != partial_result_ports.size())
        {
            WriteBufferFromOwnString out;
            if (processors && !processors->empty())
                printPipeline(*processors, out);

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add partial result transform {} to Pipe because it has {} input ports, but {} expected\n{}",
                partial_result_transform->getName(),
                inputs.size(),
                partial_result_ports.size(), out.str());
        }

        size_t next_port = 0;
        for (auto & input : inputs)
        {
            connectPartialResultPort(partial_result_ports[next_port], input);
            ++next_port;
        }

        partial_result_ports.assign(new_outputs_size, nullptr);

        next_port = 0;
        for (auto & new_partial_result_port : partial_result_transform->getOutputs())
        {
            partial_result_ports[next_port] = &new_partial_result_port;
            ++next_port;
        }

        addProcessor(std::move(partial_result_transform));
    }
}

void Pipe::connectPartialResultPort(OutputPort * partial_result_port, InputPort & partial_result_transform_port)
{
    if (partial_result_port == nullptr)
    {
        auto source = std::make_shared<NullSource>(getHeader());
        partial_result_port = &source->getPort();

        addProcessor(std::move(source));
    }

    connect(*partial_result_port, partial_result_transform_port);
}

void Pipe::addSimpleTransform(const ProcessorGetterWithStreamKind & getter)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add simple transform to empty Pipe.");

    Block new_header;

    auto add_transform = [&](OutputPort *& port, size_t partial_result_port_id, StreamType stream_type)
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
            if (stream_type == StreamType::Main)
                addPartialResultSimpleTransform(transform, partial_result_port_id);

            addProcessor(std::move(transform));
        }
    };

    size_t partial_result_port_id = 0;
    for (auto & port : output_ports)
    {
        add_transform(port, partial_result_port_id, StreamType::Main);
        ++partial_result_port_id;
    }

    add_transform(totals_port, 0, StreamType::Totals);
    add_transform(extremes_port, 0, StreamType::Extremes);

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
    dropPartialResult();

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
            addProcessor(std::move(transform));
    }

    header = std::move(new_header);
    max_parallel_streams = std::max(max_parallel_streams, max_parallel_streams_for_chains);
}

void Pipe::addProcessor(ProcessorPtr processor)
{
    if (collected_processors)
        collected_processors->emplace_back(processor);

    processors->emplace_back(std::move(processor));
}

void Pipe::resize(size_t num_streams, bool force, bool strict)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot resize an empty Pipe");

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

    for (auto & port : partial_result_ports)
        add_transform(port, StreamType::PartialResult);

    output_ports.clear();
    header.clear();
}

void Pipe::transform(const Transformer & transformer, bool check_ports)
{
    if (output_ports.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot transform empty Pipe");

    /// TODO: Add functionality to work with partial result ports in transformer.
    dropPartialResult();

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

OutputPort * Pipe::getPartialResultPort(size_t pos) const
{
    return partial_result_ports.empty() ? nullptr : partial_result_ports[pos];
}


}
