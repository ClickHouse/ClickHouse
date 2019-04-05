#include <Processors/QueryPipeline.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/NullSink.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/ExtremsTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

namespace DB
{

void QueryPipeline::checkInitialized()
{
    if (!initialized())
        throw Exception("QueryPipeline wasn't initialized.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::checkSource(const ProcessorPtr & source)
{
    if (!source->getInputs().empty())
        throw Exception("Source for query pipeline shouldn't have any input, but " + source->getName() + " has " +
                        toString(source->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (source->getOutputs().size() != 1)
        throw Exception("Source for query pipeline should have single output, but " + source->getName() + " has " +
                        toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::init(Processors sources)
{
    if (initialized())
        throw Exception("Pipeline has already been initialized.", ErrorCodes::LOGICAL_ERROR);

    if (sources.empty())
        throw Exception("Can't initialize pipeline with empty source list.", ErrorCodes::LOGICAL_ERROR);

    for (auto & source : sources)
    {
        checkSource(source);

        auto & header = source->getOutputs().front().getHeader();

        if (current_header)
            assertBlocksHaveEqualStructure(current_header, header, "QueryPipeline");
        else
            current_header = header;

        streams.emplace_back(&source->getOutputs().front());
        processors.emplace_back(std::move(source));
    }
}

void QueryPipeline::addSimpleTransform(const ProcessorGetter & getter)
{
    checkInitialized();

    Block header;

    for (auto & stream : streams)
    {
        auto transform = getter(current_header);

        if (transform->getInputs().size() != 1)
            throw Exception("Processor for query pipeline transform should have single input, "
                            "but " + transform->getName() + " has " +
                            toString(transform->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

        if (transform->getOutputs().size() != 1)
            throw Exception("Processor for query pipeline transform should have single output, "
                            "but " + transform->getName() + " has " +
                            toString(transform->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

        auto & out_header = transform->getOutputs().front().getHeader();

        if (header)
            assertBlocksHaveEqualStructure(header, out_header, "QueryPipeline");
        else
            header = out_header;

        connect(*stream, transform->getInputs().front());
        stream = &transform->getOutputs().front();
        processors.emplace_back(std::move(transform));
    }

    current_header = std::move(header);
}

void QueryPipeline::addPipe(Processors pipe)
{
    checkInitialized();
    concatDelayedStream();

    if (pipe.empty())
        throw Exception("Can't add empty processors list to QueryPipeline.", ErrorCodes::LOGICAL_ERROR);

    auto & first = pipe.front();
    auto & last = pipe.back();

    auto num_inputs = first->getInputs().size();

    if (num_inputs != streams.size())
        throw Exception("Can't add processors to QueryPipeline because first processor has " + toString(num_inputs) +
                        " input ports, but QueryPipeline has " + toString(streams.size()) + " streams.",
                        ErrorCodes::LOGICAL_ERROR);

    auto stream = streams.begin();
    for (auto & input : first->getInputs())
        connect(**(stream++), input);

    Block header;
    streams.clear();
    streams.reserve(last->getOutputs().size());
    for (auto & output : last->getOutputs())
    {
        streams.emplace_back(&output);
        if (header)
            assertBlocksHaveEqualStructure(header, output.getHeader(), "QueryPipeline");
        else
            header = output.getHeader();
    }

    processors.insert(processors.end(), pipe.begin(), pipe.end());
    current_header = std::move(header);
}

void QueryPipeline::addDelayedStream(ProcessorPtr source)
{
    checkInitialized();

    if (has_delayed_stream)
        throw Exception("QueryPipeline already has stream with non joined data.", ErrorCodes::LOGICAL_ERROR);

    checkSource(source);
    assertBlocksHaveEqualStructure(current_header, source->getOutputs().front().getHeader(), "QueryPipeline");

    has_delayed_stream = !streams.empty();
    streams.emplace_back(&source->getOutputs().front());
    processors.emplace_back(std::move(source));
}

void QueryPipeline::concatDelayedStream()
{
    if (!has_delayed_stream)
        return;

    auto resize = std::make_shared<ResizeProcessor>(current_header, getNumMainStreams(), 1);
    auto stream = streams.begin();
    for (auto & input : resize->getInputs())
        connect(**(stream++), input);

    auto concat = std::make_shared<ConcatProcessor>(current_header, 2);
    connect(resize->getOutputs().front(), concat->getInputs().front());
    connect(*streams.back(), concat->getInputs().back());

    streams = { &concat->getOutputs().front() };
    processors.emplace_back(std::move(resize));
    processors.emplace_back(std::move(concat));
    has_delayed_stream = false;
}

void QueryPipeline::resize(size_t num_streams)
{
    checkInitialized();
    concatDelayedStream();

    if (num_streams == getNumStreams())
        return;

    auto resize = std::make_shared<ResizeProcessor>(current_header, getNumStreams(), num_streams);
    auto stream = streams.begin();
    for (auto & input : resize->getInputs())
        connect(**(stream++), input);

    streams.clear();
    streams.reserve(num_streams);
    for (auto & output : resize->getOutputs())
        streams.emplace_back(&output);

    processors.emplace_back(std::move(resize));
}

void QueryPipeline::addTotalsHavingTransform(ProcessorPtr transform)
{
    checkInitialized();

    if (!typeid_cast<const TotalsHavingTransform *>(transform.get()))
        throw Exception("TotalsHavingTransform expected for QueryPipeline::addTotalsHavingTransform.",
                ErrorCodes::LOGICAL_ERROR);

    if (has_totals_having)
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    has_totals_having = true;

    resize(1);

    connect(*streams.front(), transform->getInputs().front());

    auto & outputs = transform->getOutputs();

    streams = { &outputs.front() };
    totals_having_port = &outputs.back();
    current_header = outputs.front().getHeader();
    processors.emplace_back(std::move(transform));
}

void QueryPipeline::addExtremesTransform(ProcessorPtr transform)
{
    checkInitialized();

    if (!typeid_cast<const ExtremesTransform *>(transform.get()))
        throw Exception("ExtremesTransform expected for QueryPipeline::addExtremesTransform.",
                        ErrorCodes::LOGICAL_ERROR);

    if (has_extremes)
        throw Exception("Extremes transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    has_extremes = true;

    if (getNumStreams() != 1)
        throw Exception("Cant't add Extremes transform because pipeline is expected to have single stream, "
                        "but it has " + toString(getNumStreams()) + " streams.", ErrorCodes::LOGICAL_ERROR);

    connect(*streams.front(), transform->getInputs().front());

    auto & outputs = transform->getOutputs();

    streams = { &outputs.front() };
    extremes_port = &outputs.back();
    current_header = outputs.front().getHeader();
    processors.emplace_back(std::move(transform));
}

void QueryPipeline::addCreatingSetsTransform(ProcessorPtr transform)
{
    checkInitialized();

    if (!typeid_cast<const CreatingSetsTransform *>(transform.get()))
        throw Exception("CreatingSetsTransform expected for QueryPipeline::addExtremesTransform.",
                        ErrorCodes::LOGICAL_ERROR);

    resize(1);

    auto concat = std::make_shared<ConcatProcessor>(current_header, 2);
    connect(transform->getOutputs().front(), concat->getInputs().front());
    connect(*streams.back(), concat->getInputs().back());

    streams = { &concat->getOutputs().front() };
    processors.emplace_back(std::move(transform));
    processors.emplace_back(std::move(concat));
}

void QueryPipeline::setOutput(ProcessorPtr output)
{
    checkInitialized();

    auto * format = dynamic_cast<IOutputFormat * >(output.get());

    if (!format)
        throw Exception("IOutputFormat processor expected for QueryPipeline::setOutput.", ErrorCodes::LOGICAL_ERROR);

    if (has_output)
        throw Exception("QueryPipeline already has output.", ErrorCodes::LOGICAL_ERROR);

    has_output = true;

    resize(1);

    auto & main = format->getPort(IOutputFormat::PortKind::Main);
    auto & totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!has_totals_having)
    {
        auto null_source = std::make_shared<NullSource>(totals.getHeader());
        totals_having_port = &null_source->getPort();
        processors.emplace_back(std::move(null_source));
    }

    if (!has_extremes)
    {
        auto null_source = std::make_shared<NullSource>(extremes.getHeader());
        extremes_port = &null_source->getPort();
        processors.emplace_back(std::move(null_source));
    }

    processors.emplace_back(std::move(output));

    connect(*streams.front(), main);
    connect(*totals_having_port, totals);
    connect(*extremes_port, extremes);
}

void QueryPipeline::unitePipelines(std::vector<QueryPipeline> && pipelines, const Context & context)
{
    checkInitialized();
    concatDelayedStream();

    std::vector<OutputPort *> extremes;

    for (auto & pipeline : pipelines)
    {
        pipeline.checkInitialized();
        pipeline.concatDelayedStream();

        pipeline.addSimpleTransform([&](const Block & header){
           return std::make_shared<ConvertingTransform>(
                   header, current_header, ConvertingTransform::MatchColumnsMode::Position, context);
        });

        if (pipeline.extremes_port)
        {
            auto converting = std::make_shared<ConvertingTransform>(
                pipeline.current_header, current_header, ConvertingTransform::MatchColumnsMode::Position, context);

            connect(*pipeline.extremes_port, converting->getInputPort());
            extremes.push_back(&converting->getOutputPort());
            processors.push_back(std::move(converting));
        }

        /// Take totals only from first port.
        if (pipeline.totals_having_port)
        {
            if (!has_totals_having)
            {

                has_totals_having = true;
                auto converting = std::make_shared<ConvertingTransform>(
                    pipeline.current_header, current_header, ConvertingTransform::MatchColumnsMode::Position, context);

                connect(*pipeline.extremes_port, converting->getInputPort());
                totals_having_port = &converting->getOutputPort();
                processors.push_back(std::move(converting));
            }
            else
            {
                auto null_sink = std::make_shared<NullSink>(pipeline.totals_having_port->getHeader());
                connect(*pipeline.totals_having_port, null_sink->getPort());
                processors.emplace_back(std::move(null_sink));
            }
        }

        processors.insert(processors.end(), pipeline.processors.begin(), pipeline.processors.end());
        streams.insert(streams.end(), pipeline.streams.begin(), pipeline.streams.end());
    }

    if (!extremes.empty())
    {
        has_extremes = true;
        size_t num_inputs = extremes.size() + (has_extremes ? 1u : 0u);

        if (num_inputs == 1)
            extremes_port = extremes.front();
        else
        {
            /// Add extra processor for extremes.
            auto resize = std::make_shared<ResizeProcessor>(current_header, num_inputs, 1);
            auto input = resize->getInputs().begin();

            if (has_extremes)
                connect(*extremes_port, *(input++));

            for (auto & output : extremes)
                connect(*output, *(input++));

            auto transform = std::make_shared<ExtremesTransform>(current_header);
            extremes_port = &transform->getOutputPort();

            connect(resize->getOutputs().front(), transform->getInputPort());
            processors.emplace_back(std::move(transform));
        }
    }
}

void QueryPipeline::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & processor : processors)
        if (auto * source = typeid_cast<SourceFromInputStream *>(processor.get()))
            source->getStream()->setProgressCallback(callback);
}

void QueryPipeline::setProcessListElement(QueryStatus * elem)
{
    for (auto & processor : processors)
        if (auto * source = typeid_cast<SourceFromInputStream *>(processor.get()))
            source->getStream()->setProcessListElement(elem);
}


PipelineExecutorPtr QueryPipeline::execute(size_t num_threads)
{
    checkInitialized();

    if (!has_output)
        throw Exception("Cannot execute pipeline because it doesn't have output.", ErrorCodes::LOGICAL_ERROR);

    if (executor)
        return executor;

    pool = std::make_shared<ThreadPool>(num_threads, num_threads, num_threads);
    executor = std::make_shared<PipelineExecutor>(processors, pool.get());

    return executor;
}

}
