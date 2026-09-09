#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

constexpr size_t rows_per_chunk = 1000;
constexpr size_t chunks_per_stream = 4;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(type->createColumn(), type, "k")});
}

/// Distinct keys for every stream, so the hash spreads them over all buckets.
Chunks makeChunks(size_t stream_index)
{
    Chunks chunks;
    for (size_t chunk_index = 0; chunk_index < chunks_per_stream; ++chunk_index)
    {
        auto column = ColumnUInt64::create();
        const size_t first_key = (stream_index * chunks_per_stream + chunk_index) * rows_per_chunk;
        for (size_t row = 0; row < rows_per_chunk; ++row)
            column->insertValue(first_key + row);
        Columns columns;
        columns.emplace_back(std::move(column));
        chunks.emplace_back(std::move(columns), rows_per_chunk);
    }
    return chunks;
}

/// Every task of the test is bucket 0.
class FixedBucketParameterLookup : public IParameterLookup
{
public:
    Field getParameter(const String & name) const override
    {
        chassert(name == "bucket_id");
        return "0";
    }
};

class CountingSink : public ISink
{
public:
    explicit CountingSink(SharedHeader header) : ISink(std::move(header)) {}
    String getName() const override { return "CountingSink"; }

protected:
    void consume(Chunk) override {}
};

/// The streaming exchange without sockets: the real serializer, sinks that discard the packets, and
/// sources that replay the chunks of the sending bucket.
class TestExchangeLookup : public IExchangeLookup
{
public:
    std::shared_ptr<IProcessor> createSerializer(SharedHeader input_header, const String &) override
    {
        return std::make_shared<StreamingExchangeSerializingTransform>(std::move(input_header));
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId &) override
    {
        return std::make_shared<CountingSink>(std::move(input_header));
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & stream_id) override
    {
        return std::make_shared<SourceFromChunks>(std::move(output_header), makeChunks(parse<size_t>(stream_id.source_bucket)));
    }
};

BuildQueryPipelineSettings makeSettings(ContextPtr context, size_t max_threads)
{
    BuildQueryPipelineSettings settings(context);
    settings.max_threads = max_threads;
    settings.parameter_lookup = std::make_shared<FixedBucketParameterLookup>();
    settings.exchange_lookup = std::make_shared<TestExchangeLookup>();
    return settings;
}

size_t countProcessors(const Processors & processors, const String & name)
{
    size_t count = 0;
    for (const auto & processor : processors)
        if (processor->getName() == name)
            ++count;
    return count;
}

/// The step that consumes a shuffle bucket: GROUP BY k without aggregate functions.
std::unique_ptr<AggregatingStep> makeAggregatingStep(const SharedHeader & header)
{
    Aggregator::Params params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/ false,
        /*max_threads=*/ 1,
        /*max_block_size=*/ 65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/ 0.5f,
        /*serialize_string_with_zero_byte=*/ false,
        /*enable_packed_string_keys=*/ true);

    return std::make_unique<AggregatingStep>(
        header,
        std::move(params),
        GroupingSetsParamsList{},
        /*final=*/ true,
        /*max_block_size=*/ 65536,
        /*aggregation_in_order_max_block_bytes=*/ 0,
        /*merge_threads=*/ 1,
        /*temporary_data_merge_threads=*/ 1,
        /*storage_has_evenly_distributed_read=*/ false,
        /*group_by_use_nulls=*/ false,
        /*sort_description_for_merging=*/ SortDescription{},
        /*group_by_sort_description=*/ SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/ false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/ false,
        /*explicit_sorting_required_for_aggregation_in_order=*/ false);
}

/// Statistics of a sending pipeline, collected by `runSendingStep`.
struct SendingStats
{
    /// Data rows received by the busiest processor behind the sources, and its name. Processors
    /// that receive serialized packets have another header and are not counted.
    size_t max_rows_into_one_processor = 0;
    String max_rows_processor;
    size_t rows_in_sinks = 0;
    size_t sinks = 0;
    size_t serializers = 0;
};

/// Feeds `num_streams` sources with `makeChunks` into the sending step, runs the pipeline on
/// `num_streams` threads and collects the statistics.
SendingStats runSendingStep(IQueryPlanStep & step, size_t num_streams, const SharedHeader & header, const BuildQueryPipelineSettings & settings)
{
    Pipes pipes;
    for (size_t stream = 0; stream < num_streams; ++stream)
        pipes.emplace_back(std::make_shared<SourceFromChunks>(header, makeChunks(stream)));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));
    EXPECT_EQ(builder->getNumStreams(), num_streams);

    QueryPipelineBuilders builders;
    builders.emplace_back(std::move(builder));
    auto sending = step.updatePipeline(std::move(builders), settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*sending));
    pipeline.setNumThreads(num_streams);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    SendingStats stats;
    for (const auto & processor : pipeline.getProcessors())
    {
        if (processor->getInputs().empty())
            continue;

        const size_t input_rows = processor->getProcessorDataStats().input_rows;
        if (processor->getName() == "CountingSink")
        {
            ++stats.sinks;
            stats.rows_in_sinks += input_rows;
        }
        if (processor->getName() == "StreamingExchangeSerializingTransform")
            ++stats.serializers;

        if (!blocksHaveEqualStructure(processor->getInputs().front().getHeader(), *header))
            continue;
        if (input_rows > stats.max_rows_into_one_processor)
        {
            stats.max_rows_into_one_processor = input_rows;
            stats.max_rows_processor = processor->getName();
        }
    }
    return stats;
}

constexpr size_t streams = 8;
constexpr size_t total_rows = streams * chunks_per_stream * rows_per_chunk;
constexpr size_t rows_per_stream = total_rows / streams;

/// The work after the sources must stay spread over the streams: no processor may receive more than
/// one stream's worth of rows, plus a margin for uneven hashing.
void expectSpreadOverStreams(const SendingStats & stats)
{
    EXPECT_LE(stats.max_rows_into_one_processor, rows_per_stream * 3 / 2)
        << stats.max_rows_processor << " received " << stats.max_rows_into_one_processor << " of " << total_rows
        << " rows, one stream carries " << rows_per_stream;
}

}

/// A receiving task gets one exchange source per sending task. The aggregation after it must
/// still run on `max_threads` streams: with 3 senders and 8 threads, 8 `AggregatingTransform`,
/// not 3.
TEST(ShuffleExchangeParallelism, ReceiverSpreadsInputOverMaxThreads)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    constexpr size_t senders = 3;
    constexpr size_t max_threads = 8;

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, max_threads);
    auto header = makeHeader();

    Strings source_shards;
    for (size_t sender = 0; sender < senders; ++sender)
        source_shards.push_back(toString(sender));

    ShuffleReceiveStep receive(header, "exchange_0", source_shards);
    QueryPipelineBuilder builder;
    receive.initializePipeline(builder, settings);
    EXPECT_EQ(countProcessors(builder.getProcessors(), "SourceFromChunks"), senders);

    auto aggregating = makeAggregatingStep(header);
    aggregating->transformPipeline(builder, settings);

    EXPECT_EQ(countProcessors(builder.getProcessors(), "AggregatingTransform"), max_threads);
}

/// A sending task scatters its read streams into the destination buckets. The work after the
/// scatter must stay spread over the streams; otherwise a third of all rows would go through one
/// processor on one thread while 8 threads are available.
TEST(ShuffleExchangeParallelism, SenderKeepsBucketWorkSpreadOverStreams)
{
    MainThreadStatus::getInstance();

    constexpr size_t buckets = 3;
    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    ShuffleSendStep send(header, "exchange_0", Names{"k"}, buckets);
    auto stats = runSendingStep(send, streams, header, settings);

    expectSpreadOverStreams(stats);
    EXPECT_EQ(stats.sinks, buckets);
    /// The packets keep the row count of the data they carry.
    EXPECT_EQ(stats.rows_in_sinks, total_rows);
}

/// A gather sends everything to one destination. When no order has to be kept, the serialization
/// must still run on every stream ahead of the merge into the single sink.
TEST(ShuffleExchangeParallelism, UnsortedGatherKeepsSerializationSpreadOverStreams)
{
    MainThreadStatus::getInstance();

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    GatherSendStep gather(header, "exchange_0");
    auto stats = runSendingStep(gather, streams, header, settings);

    expectSpreadOverStreams(stats);
    EXPECT_EQ(stats.serializers, streams);
    EXPECT_EQ(stats.sinks, 1u);
    EXPECT_EQ(stats.rows_in_sinks, total_rows);
}

/// A sorted gather merges the streams first, so the serialization can only follow the merge:
/// the single sink serializes itself and no serializer sits in front of the merge.
TEST(ShuffleExchangeParallelism, SortedGatherSerializesAfterTheMerge)
{
    MainThreadStatus::getInstance();

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    SortDescription by_key;
    by_key.emplace_back("k", 1, 1);
    GatherSendStep gather(header, "exchange_0", by_key);
    auto stats = runSendingStep(gather, streams, header, settings);

    EXPECT_EQ(stats.serializers, 0u);
    EXPECT_EQ(stats.sinks, 1u);
    EXPECT_EQ(stats.rows_in_sinks, total_rows);
}
