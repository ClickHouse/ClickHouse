#include <Processors/QueryPipeline.h>
#include <Processors/IProcessor.h>
#include <Processors/Pipe.h>
#include <Processors/Chain.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sinks/ExceptionHandlingSink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Formats/IOutputFormat.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>

namespace DB
{

QueryPipeline::QueryPipeline() = default;
QueryPipeline::QueryPipeline(QueryPipeline &&) = default;
QueryPipeline & QueryPipeline::operator=(QueryPipeline &&) = default;
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

QueryPipeline::QueryPipeline(
    PipelineResourcesHolder resources_,
    Processors processors_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
{
    for (const auto & processor : processors)
    {
        for (const auto & in : processor->getInputs())
            checkInput(in, processor);

        for (const auto & out : processor->getOutputs())
            checkOutput(out, processor);
    }
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
    if (!output || output->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its output port is connected or null");

    if (totals && totals->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its totals port is connected");

    if (extremes || extremes->isConnected())
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

QueryPipeline::QueryPipeline(std::shared_ptr<ISource> source) : QueryPipeline(Pipe(std::move(source))) {}

QueryPipeline::QueryPipeline(Pipe pipe)
    : QueryPipeline(std::move(pipe.holder), std::move(pipe.processors), pipe.getOutputPort(0), pipe.getTotalsPort(), pipe.getExtremesPort())
{
}

QueryPipeline::QueryPipeline(Chain chain)
    : resources(chain.detachResources())
    , input(&chain.getInputPort())
    , num_threads(chain.getNumThreads())
{
    processors.reserve(chain.getProcessors().size() + 1);
    for (auto processor : chain.getProcessors())
        processors.emplace_back(std::move(processor));

    auto sink = std::make_shared<ExceptionHandlingSink>(chain.getOutputPort().getHeader());
    connect(chain.getOutputPort(), sink->getPort());
    processors.emplace_back(std::move(sink));

    input = &chain.getInputPort();
}

QueryPipeline::QueryPipeline(std::shared_ptr<SinkToStorage> sink) : QueryPipeline(Chain(std::move(sink))) {}

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

void QueryPipeline::complete(std::shared_ptr<IOutputFormat> format)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with output format");

    if (format->expectMaterializedColumns())
    {
        auto materializing = std::make_shared<MaterializingTransform>(output->getHeader());
        connect(*output, materializing->getInputPort());
        output = &materializing->getOutputPort();
        processors.emplace_back(std::move(output));
    }

    auto & format_main = format->getPort(IOutputFormat::PortKind::Main);
    auto & format_totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & format_extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals)
    {
        auto source = std::make_shared<NullSource>(totals->getHeader());
        totals = &source->getPort();
        processors.emplace_back(std::move(source));
    }

    if (!extremes)
    {
        auto source = std::make_shared<NullSource>(extremes->getHeader());
        extremes = &source->getPort();
        processors.emplace_back(std::move(source));
    }

    processors.emplace_back(std::move(format));

    connect(*output, format_main);
    connect(*totals, format_totals);
    connect(*extremes, format_extremes);

    output = nullptr;
    totals = nullptr;
    extremes = nullptr;
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

    if (pulling())
    {
        for (auto & processor : processors)
        {
            if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
                source->setProcessListElement(elem);
        }
    }
    else if (pushing())
    {
        if (auto * counting = dynamic_cast<CountingTransform *>(&input->getOutputPort().getProcessor()))
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
            "It is possible to set limits and quota only to pullint QueryPipeline");

    auto transform = std::make_shared<LimitsCheckingTransform>(output->getHeader(), limits);
    transform->setQuota(quota);
    connect(*output, transform->getInputPort());
    output = &transform->getOutputPort();
    processors.emplace_back(std::move(transform));
}


bool QueryPipeline::tryGetResultRowsAndBytes(size_t & result_rows, size_t & result_bytes) const
{
    if (!output || !output->isConnected())
        return false;

    const auto * format = typeid_cast<const IOutputFormat *>(&output->getInputPort().getProcessor());
    if (!format)
        return false;

    result_rows = format->getResultRows();
    result_bytes = format->getResultBytes();
    return true;
}

void QueryPipeline::reset()
{
    QueryPipeline to_remove = std::move(*this);
    *this = QueryPipeline();
}

}
