#include <Processors/QueryPipeline.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/NullSink.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

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

static ProcessorPtr callProcessorGetter(
    const Block & header, const QueryPipeline::ProcessorGetter & getter, QueryPipeline::StreamType)
{
    return getter(header);
}

static ProcessorPtr callProcessorGetter(
    const Block & header, const QueryPipeline::ProcessorGetterWithStreamKind & getter, QueryPipeline::StreamType kind)
{
    return getter(header, kind);
}

template <typename TProcessorGetter>
void QueryPipeline::addSimpleTransformImpl(const TProcessorGetter & getter)
{
    checkInitialized();

    Block header;

    auto add_transform = [&](OutputPort *& stream, StreamType stream_type)
    {
        if (!stream)
            return;

        auto transform = callProcessorGetter(current_header, getter, stream_type);

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
    };

    for (auto & stream : streams)
        add_transform(stream, StreamType::Main);

    add_transform(delayed_stream_port, StreamType::Main);
    add_transform(totals_having_port, StreamType::Totals);
    add_transform(extremes_port, StreamType::Extremes);

    current_header = std::move(header);
}

void QueryPipeline::addSimpleTransform(const ProcessorGetter & getter)
{
    addSimpleTransformImpl(getter);
}

void QueryPipeline::addSimpleTransform(const ProcessorGetterWithStreamKind & getter)
{
    addSimpleTransformImpl(getter);
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

    if (delayed_stream_port)
        throw Exception("QueryPipeline already has stream with non joined data.", ErrorCodes::LOGICAL_ERROR);

    checkSource(source);
    assertBlocksHaveEqualStructure(current_header, source->getOutputs().front().getHeader(), "QueryPipeline");

    delayed_stream_port = &source->getOutputs().front();
    processors.emplace_back(std::move(source));
}

void QueryPipeline::concatDelayedStream()
{
    if (!delayed_stream_port)
        return;

    auto resize = std::make_shared<ResizeProcessor>(current_header, getNumMainStreams(), 1);
    auto stream = streams.begin();
    for (auto & input : resize->getInputs())
        connect(**(stream++), input);

    auto concat = std::make_shared<ConcatProcessor>(current_header, 2);
    connect(resize->getOutputs().front(), concat->getInputs().front());
    connect(*delayed_stream_port, concat->getInputs().back());

    streams = { &concat->getOutputs().front() };
    processors.emplace_back(std::move(resize));
    processors.emplace_back(std::move(concat));

    delayed_stream_port = nullptr;
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

    if (totals_having_port)
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    resize(1);

    connect(*streams.front(), transform->getInputs().front());

    auto & outputs = transform->getOutputs();

    streams = { &outputs.front() };
    totals_having_port = &outputs.back();
    current_header = outputs.front().getHeader();
    processors.emplace_back(std::move(transform));
}

void QueryPipeline::addDefaultTotals()
{
    checkInitialized();

    if (totals_having_port)
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    Columns columns;
    columns.reserve(current_header.columns());

    for (size_t i = 0; i < current_header.columns(); ++i)
    {
        auto column = current_header.getByPosition(i).type->createColumn();
        column->insertDefault();
        columns.emplace_back(std::move(column));
    }

    auto source = std::make_shared<SourceFromSingleChunk>(current_header, Chunk(std::move(columns), 1));
    totals_having_port = &source->getPort();
    processors.emplace_back(source);
}

void QueryPipeline::addTotals(ProcessorPtr source)
{
    checkInitialized();

    if (totals_having_port)
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    checkSource(source);
    assertBlocksHaveEqualStructure(current_header, source->getOutputs().front().getHeader(), "QueryPipeline");

    totals_having_port = &source->getOutputs().front();
    processors.emplace_back(source);
}

void QueryPipeline::addExtremesTransform(ProcessorPtr transform)
{
    checkInitialized();

    if (!typeid_cast<const ExtremesTransform *>(transform.get()))
        throw Exception("ExtremesTransform expected for QueryPipeline::addExtremesTransform.",
                        ErrorCodes::LOGICAL_ERROR);

    if (extremes_port)
        throw Exception("Extremes transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

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

    if (output_format)
        throw Exception("QueryPipeline already has output.", ErrorCodes::LOGICAL_ERROR);

    output_format = format;

    resize(1);

    auto & main = format->getPort(IOutputFormat::PortKind::Main);
    auto & totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals_having_port)
    {
        auto null_source = std::make_shared<NullSource>(totals.getHeader());
        totals_having_port = &null_source->getPort();
        processors.emplace_back(std::move(null_source));
    }

    if (!extremes_port)
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

void QueryPipeline::unitePipelines(
    std::vector<QueryPipeline> && pipelines, const Block & common_header, const Context & context)
{
    checkInitialized();
    concatDelayedStream();

    addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ConvertingTransform>(
                header, common_header, ConvertingTransform::MatchColumnsMode::Position, context);
    });

    std::vector<OutputPort *> extremes;

    for (auto & pipeline : pipelines)
    {
        pipeline.checkInitialized();
        pipeline.concatDelayedStream();

        pipeline.addSimpleTransform([&](const Block & header)
        {
           return std::make_shared<ConvertingTransform>(
                   header, common_header, ConvertingTransform::MatchColumnsMode::Position, context);
        });

        if (pipeline.extremes_port)
        {
            auto converting = std::make_shared<ConvertingTransform>(
                pipeline.current_header, common_header, ConvertingTransform::MatchColumnsMode::Position, context);

            connect(*pipeline.extremes_port, converting->getInputPort());
            extremes.push_back(&converting->getOutputPort());
            processors.push_back(std::move(converting));
        }

        /// Take totals only from first port.
        if (pipeline.totals_having_port)
        {
            if (!totals_having_port)
            {
                auto converting = std::make_shared<ConvertingTransform>(
                    pipeline.current_header, common_header, ConvertingTransform::MatchColumnsMode::Position, context);

                connect(*pipeline.totals_having_port, converting->getInputPort());
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
        size_t num_inputs = extremes.size() + (extremes_port ? 1u : 0u);

        if (num_inputs == 1)
            extremes_port = extremes.front();
        else
        {
            /// Add extra processor for extremes.
            auto resize = std::make_shared<ResizeProcessor>(current_header, num_inputs, 1);
            auto input = resize->getInputs().begin();

            if (extremes_port)
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
    {
        if (auto * source = typeid_cast<SourceFromInputStream *>(processor.get()))
            source->getStream().setProgressCallback(callback);

        if (auto * source = typeid_cast<CreatingSetsTransform *>(processor.get()))
            source->setProgressCallback(callback);
    }
}

void QueryPipeline::setProcessListElement(QueryStatus * elem)
{
    for (auto & processor : processors)
    {
        if (auto * source = typeid_cast<SourceFromInputStream *>(processor.get()))
            source->getStream().setProcessListElement(elem);

        if (auto * source = typeid_cast<CreatingSetsTransform *>(processor.get()))
            source->setProcessListElement(elem);
    }
}

void QueryPipeline::finalize()
{
    checkInitialized();

    if (!output_format)
        throw Exception("Cannot finalize pipeline because it doesn't have output.", ErrorCodes::LOGICAL_ERROR);

    calcRowsBeforeLimit();
}

void QueryPipeline::calcRowsBeforeLimit()
{
    /// TODO get from Remote

    UInt64 rows_before_limit_at_least = 0;
    bool has_limit = false;

    for (auto & processor : processors)
    {
        if (auto * limit = typeid_cast<const LimitTransform *>(processor.get()))
        {
            has_limit = true;
            rows_before_limit_at_least += limit->getRowsBeforeLimitAtLeast();
        }

        if (auto * source = typeid_cast<SourceFromInputStream *>(processor.get()))
        {
            auto & info = source->getStream().getProfileInfo();
            if (info.hasAppliedLimit())
            {
                has_limit = true;
                rows_before_limit_at_least += info.getRowsBeforeLimit();
            }
        }
    }

    UInt64 rows_before_limit = 0;
    bool has_partial_sorting = false;

    for (auto & processor : processors)
    {
        if (auto * limit = typeid_cast<const PartialSortingTransform *>(processor.get()))
        {
            has_partial_sorting = true;
            rows_before_limit += limit->getNumReadRows();
        }
    }

    if (has_limit)
        output_format->setRowsBeforeLimit(has_partial_sorting ? rows_before_limit : rows_before_limit_at_least);
}

PipelineExecutorPtr QueryPipeline::execute(size_t num_threads)
{
    checkInitialized();

    if (!output_format)
        throw Exception("Cannot execute pipeline because it doesn't have output.", ErrorCodes::LOGICAL_ERROR);

    if (executor)
        return executor;

    pool = std::make_shared<ThreadPool>(num_threads, num_threads, num_threads);
    executor = std::make_shared<PipelineExecutor>(processors, pool.get());

    return executor;
}

}
