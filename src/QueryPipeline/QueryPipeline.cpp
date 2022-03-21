#include <queue>
#include <QueryPipeline/Chain.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/IProcessor.h>
#include <Processors/LimitTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

QueryPipeline::QueryPipeline() = default;
QueryPipeline::QueryPipeline(QueryPipeline &&) noexcept = default;
QueryPipeline & QueryPipeline::operator=(QueryPipeline &&) noexcept = default;
QueryPipeline::~QueryPipeline() = default;

static void checkInput(const InputPort & input, const ProcessorPtr & processor)
{
    if (!input.isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create QueryPipeline because {} has not connected input",
            processor->getName());
}

static void checkOutput(const OutputPort & output, const ProcessorPtr & processor)
{
    if (!output.isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create QueryPipeline because {} has not connected output",
            processor->getName());
}

static void checkPulling(
    Processors & processors,
    OutputPort * output,
    OutputPort * totals,
    OutputPort * extremes)
{
    if (!output || output->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its output port is connected or null");

    if (totals && totals->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its totals port is connected");

    if (extremes && extremes->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its extremes port is connected");

    bool found_output = false;
    bool found_totals = false;
    bool found_extremes = false;
    for (const auto & processor : processors)
    {
        for (const auto & in : processor->getInputs())
            checkInput(in, processor);

        for (const auto & out : processor->getOutputs())
        {
            if (&out == output)
                found_output = true;
            else if (totals && &out == totals)
                found_totals = true;
            else if (extremes && &out == extremes)
                found_extremes = true;
            else
                checkOutput(out, processor);
        }
    }

    if (!found_output)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its output port does not belong to any processor");
    if (totals && !found_totals)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its totals port does not belong to any processor");
    if (extremes && !found_extremes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its extremes port does not belong to any processor");
}

static void checkCompleted(Processors & processors)
{
    for (const auto & processor : processors)
    {
        for (const auto & in : processor->getInputs())
            checkInput(in, processor);

        for (const auto & out : processor->getOutputs())
            checkOutput(out, processor);
    }
}

static void initRowsBeforeLimit(IOutputFormat * output_format)
{
    RowsBeforeLimitCounterPtr rows_before_limit_at_least;

    /// TODO: add setRowsBeforeLimitCounter as virtual method to IProcessor.
    std::vector<LimitTransform *> limits;
    std::vector<RemoteSource *> remote_sources;

    std::unordered_set<IProcessor *> visited;

    struct QueuedEntry
    {
        IProcessor * processor;
        bool visited_limit;
    };

    std::queue<QueuedEntry> queue;

    queue.push({ output_format, false });
    visited.emplace(output_format);

    while (!queue.empty())
    {
        auto * processor = queue.front().processor;
        auto visited_limit = queue.front().visited_limit;
        queue.pop();

        if (!visited_limit)
        {
            if (auto * limit = typeid_cast<LimitTransform *>(processor))
            {
                visited_limit = true;
                limits.emplace_back(limit);
            }

            if (auto * source = typeid_cast<RemoteSource *>(processor))
                remote_sources.emplace_back(source);
        }
        else if (auto * sorting = typeid_cast<PartialSortingTransform *>(processor))
        {
            if (!rows_before_limit_at_least)
                rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

            sorting->setRowsBeforeLimitCounter(rows_before_limit_at_least);

            /// Don't go to children. Take rows_before_limit from last PartialSortingTransform.
            continue;
        }

        /// Skip totals and extremes port for output format.
        if (auto * format = dynamic_cast<IOutputFormat *>(processor))
        {
            auto * child_processor = &format->getPort(IOutputFormat::PortKind::Main).getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, visited_limit });

            continue;
        }

        for (auto & child_port : processor->getInputs())
        {
            auto * child_processor = &child_port.getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, visited_limit });
        }
    }

    if (!rows_before_limit_at_least && (!limits.empty() || !remote_sources.empty()))
    {
        rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

        for (auto & limit : limits)
            limit->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        for (auto & source : remote_sources)
            source->setRowsBeforeLimitCounter(rows_before_limit_at_least);
    }

    /// If there is a limit, then enable rows_before_limit_at_least
    /// It is needed when zero rows is read, but we still want rows_before_limit_at_least in result.
    if (!limits.empty())
        rows_before_limit_at_least->add(0);

    if (rows_before_limit_at_least)
        output_format->setRowsBeforeLimitCounter(rows_before_limit_at_least);
}


QueryPipeline::QueryPipeline(
    PipelineResourcesHolder resources_,
    Processors processors_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
{
    checkCompleted(processors);
}

QueryPipeline::QueryPipeline(
    PipelineResourcesHolder resources_,
    Processors processors_,
    InputPort * input_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
    , input(input_)
{
    if (!input || input->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pushing QueryPipeline because its input port is connected or null");

    bool found_input = false;
    for (const auto & processor : processors)
    {
        for (const auto & in : processor->getInputs())
        {
            if (&in == input)
                found_input = true;
            else
                checkInput(in, processor);
        }

        for (const auto & out : processor->getOutputs())
            checkOutput(out, processor);
    }

    if (!found_input)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pushing QueryPipeline because its input port does not belong to any processor");
}

QueryPipeline::QueryPipeline(std::shared_ptr<ISource> source) : QueryPipeline(Pipe(std::move(source))) {}

QueryPipeline::QueryPipeline(
    PipelineResourcesHolder resources_,
    Processors processors_,
    OutputPort * output_,
    OutputPort * totals_,
    OutputPort * extremes_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
    , output(output_)
    , totals(totals_)
    , extremes(extremes_)
{
    checkPulling(processors, output, totals, extremes);
}

QueryPipeline::QueryPipeline(Pipe pipe)
{
    resources = std::move(pipe.holder);

    if (pipe.numOutputPorts() > 0)
    {
        pipe.resize(1);
        output = pipe.getOutputPort(0);
        totals = pipe.getTotalsPort();
        extremes = pipe.getExtremesPort();

        processors = std::move(pipe.processors);
        checkPulling(processors, output, totals, extremes);
    }
    else
    {
        processors = std::move(pipe.processors);
        checkCompleted(processors);
    }
}

QueryPipeline::QueryPipeline(Chain chain)
    : resources(chain.detachResources())
    , input(&chain.getInputPort())
    , num_threads(chain.getNumThreads())
{
    processors.reserve(chain.getProcessors().size() + 1);
    for (auto processor : chain.getProcessors())
        processors.emplace_back(std::move(processor));

    auto sink = std::make_shared<EmptySink>(chain.getOutputPort().getHeader());
    connect(chain.getOutputPort(), sink->getPort());
    processors.emplace_back(std::move(sink));

    input = &chain.getInputPort();
}

QueryPipeline::QueryPipeline(std::shared_ptr<IOutputFormat> format)
{
    auto & format_main = format->getPort(IOutputFormat::PortKind::Main);
    auto & format_totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & format_extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals)
    {
        auto source = std::make_shared<NullSource>(format_totals.getHeader());
        totals = &source->getPort();
        processors.emplace_back(std::move(source));
    }

    if (!extremes)
    {
        auto source = std::make_shared<NullSource>(format_extremes.getHeader());
        extremes = &source->getPort();
        processors.emplace_back(std::move(source));
    }

    connect(*totals, format_totals);
    connect(*extremes, format_extremes);

    input = &format_main;
    totals = nullptr;
    extremes = nullptr;

    output_format = format.get();

    processors.emplace_back(std::move(format));
}

static void drop(OutputPort *& port, Processors & processors)
{
    if (!port)
        return;

    auto null_sink = std::make_shared<NullSink>(port->getHeader());
    connect(*port, null_sink->getPort());

    processors.emplace_back(std::move(null_sink));
    port = nullptr;
}

QueryPipeline::QueryPipeline(std::shared_ptr<SinkToStorage> sink) : QueryPipeline(Chain(std::move(sink))) {}

void QueryPipeline::complete(std::shared_ptr<ISink> sink)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with chain");

    drop(totals, processors);
    drop(extremes, processors);

    connect(*output, sink->getPort());
    processors.emplace_back(std::move(sink));
    output = nullptr;
}

void QueryPipeline::complete(Chain chain)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with chain");

    resources = chain.detachResources();

    drop(totals, processors);
    drop(extremes, processors);

    processors.reserve(processors.size() + chain.getProcessors().size() + 1);
    for (auto processor : chain.getProcessors())
        processors.emplace_back(std::move(processor));

    auto sink = std::make_shared<EmptySink>(chain.getOutputPort().getHeader());
    connect(*output, chain.getInputPort());
    connect(chain.getOutputPort(), sink->getPort());
    processors.emplace_back(std::move(sink));
    output = nullptr;
}

void QueryPipeline::complete(std::shared_ptr<SinkToStorage> sink)
{
    complete(Chain(std::move(sink)));
}

void QueryPipeline::complete(Pipe pipe)
{
    if (!pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pushing to be completed with pipe");

    pipe.resize(1);
    resources = pipe.detachResources();
    pipe.dropExtremes();
    pipe.dropTotals();
    connect(*pipe.getOutputPort(0), *input);
    input = nullptr;

    auto pipe_processors = Pipe::detachProcessors(std::move(pipe));
    processors.insert(processors.end(), pipe_processors.begin(), pipe_processors.end());
}

static void addMaterializing(OutputPort *& output, Processors & processors)
{
    if (!output)
        return;

    auto materializing = std::make_shared<MaterializingTransform>(output->getHeader());
    connect(*output, materializing->getInputPort());
    output = &materializing->getOutputPort();
    processors.emplace_back(std::move(materializing));
}

void QueryPipeline::complete(std::shared_ptr<IOutputFormat> format)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with output format");

    if (format->expectMaterializedColumns())
    {
        addMaterializing(output, processors);
        addMaterializing(totals, processors);
        addMaterializing(extremes, processors);
    }

    auto & format_main = format->getPort(IOutputFormat::PortKind::Main);
    auto & format_totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & format_extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals)
    {
        auto source = std::make_shared<NullSource>(format_totals.getHeader());
        totals = &source->getPort();
        processors.emplace_back(std::move(source));
    }

    if (!extremes)
    {
        auto source = std::make_shared<NullSource>(format_extremes.getHeader());
        extremes = &source->getPort();
        processors.emplace_back(std::move(source));
    }

    connect(*output, format_main);
    connect(*totals, format_totals);
    connect(*extremes, format_extremes);

    output = nullptr;
    totals = nullptr;
    extremes = nullptr;

    initRowsBeforeLimit(format.get());
    output_format = format.get();

    processors.emplace_back(std::move(format));
}

Block QueryPipeline::getHeader() const
{
    if (input)
        return input->getHeader();
    else if (output)
        return output->getHeader();
    else
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Header is available only for pushing or pulling QueryPipeline");
}

void QueryPipeline::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & processor : processors)
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProgressCallback(callback);
    }
}

void QueryPipeline::setProcessListElement(QueryStatus * elem)
{
    process_list_element = elem;

    if (pulling() || completed())
    {
        for (auto & processor : processors)
        {
            if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
                source->setProcessListElement(elem);
        }
    }
    else if (pushing())
    {
        if (auto * counting = dynamic_cast<CountingTransform *>(&input->getProcessor()))
        {
            counting->setProcessListElement(elem);
        }
    }
}


void QueryPipeline::setLimitsAndQuota(const StreamLocalLimits & limits, std::shared_ptr<const EnabledQuota> quota)
{
    if (!pulling())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "It is possible to set limits and quota only to pulling QueryPipeline");

    auto transform = std::make_shared<LimitsCheckingTransform>(output->getHeader(), limits);
    transform->setQuota(quota);
    connect(*output, transform->getInputPort());
    output = &transform->getOutputPort();
    processors.emplace_back(std::move(transform));
}


bool QueryPipeline::tryGetResultRowsAndBytes(UInt64 & result_rows, UInt64 & result_bytes) const
{
    if (!output_format)
        return false;

    result_rows = output_format->getResultRows();
    result_bytes = output_format->getResultBytes();
    return true;
}

void QueryPipeline::addStorageHolder(StoragePtr storage)
{
    resources.storage_holders.emplace_back(std::move(storage));
}

void QueryPipeline::reset()
{
    QueryPipeline to_remove = std::move(*this);
    *this = QueryPipeline();
}

}
