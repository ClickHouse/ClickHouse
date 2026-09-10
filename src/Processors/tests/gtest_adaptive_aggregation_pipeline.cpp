#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <functional>
#include <future>
#include <map>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/AdaptiveAggregationChunkInfo.h>
#include <Interpreters/AdaptiveAggregationExecution.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AdaptiveAggregatingTransform.h>
#include <Processors/Transforms/AdaptiveAggregationPartitionTransform.h>
#include <Processors/Transforms/AdaptiveAggregationCoalescingTransform.h>
#include <Processors/Transforms/AdaptiveAggregationPublishTransform.h>
#include <Processors/Transforms/AdaptiveAggregationMergeTransform.h>
#include <base/scope_guard.h>

using namespace DB;

namespace DB::FailPoints
{
extern const char adaptive_aggregation_before_spill_budget_wait[];
}

namespace ProfileEvents
{
extern const Event AdaptiveAggregationPressureDrainedRecords;
}

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key")});
}

AggregatingTransformParamsPtr makeParams(
    const SharedHeader & header, size_t external_threshold = 0, const String & aggregate = {}, TemporaryDataOnDiskScopePtr tmp_data = {})
{
    AggregateDescriptions aggregates;
    if (!aggregate.empty())
    {
        tryRegisterAggregateFunctions();
        AggregateDescription description;
        AggregateFunctionProperties properties;
        DataTypes arguments;
        if (aggregate != "count")
        {
            description.argument_names = {"key"};
            arguments = {header->getByName("key").type};
        }
        description.function = AggregateFunctionFactory::instance().get(aggregate, NullsAction::EMPTY, arguments, {}, properties);
        description.column_name = aggregate;
        aggregates.push_back(std::move(description));
    }
    Aggregator::Params params(
        Names{"key"}, aggregates, /*overflow_row_=*/false, /*max_threads_=*/2,
        /*max_block_size_=*/65536, /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5f,
        /*serialize_string_with_zero_byte_=*/false, /*enable_packed_string_keys_=*/true);
    params.max_bytes_before_external_group_by = external_threshold;
    params.tmp_data_scope = std::move(tmp_data);
    params.only_merge = false;
    params.enable_adaptive_aggregator = true;
    params.adaptive_aggregator_freeze_threshold = 64;
    return std::make_shared<AggregatingTransformParams>(header, params, /*final_=*/true);
}


struct StagingPipeline
{
    std::shared_ptr<AdaptiveAggregationPartitionTransform> partition;
    std::shared_ptr<AdaptiveAggregationCoalescingTransform> coalescing;
    std::shared_ptr<AdaptiveAggregationPublishTransform> publisher;
    std::array<bool, 3> finished{};

    StagingPipeline(const AggregatingTransformParamsPtr & params, const AdaptiveAggregationSessionPtr & session)
        : partition(std::make_shared<AdaptiveAggregationPartitionTransform>(params, session))
        , coalescing(std::make_shared<AdaptiveAggregationCoalescingTransform>(params, session))
        , publisher(std::make_shared<AdaptiveAggregationPublishTransform>(params, session))
    {
        connect(partition->getOutputPort(), coalescing->getInputs().front());
        connect(coalescing->getOutputs().front(), publisher->getInputs().front());
    }

    InputPort & input() { return partition->getInputPort(); }
    OutputPort & output() { return publisher->getOutputs().front(); }

    void prime()
    {
        ASSERT_EQ(publisher->prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(coalescing->prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(partition->prepare(), IProcessor::Status::NeedData);
    }

    /// Advance actual processors until the requested port transition, with a bound to expose deadlocks.
    template <typename Predicate>
    bool runUntil(Predicate && done)
    {
        for (size_t iteration = 0; iteration < 100; ++iteration)
        {
            const std::array<IProcessor *, 3> processors{publisher.get(), coalescing.get(), partition.get()};
            for (size_t i = 0; i < processors.size(); ++i)
            {
                if (finished[i])
                    continue;
                const auto status = processors[i]->prepare();
                if (status == IProcessor::Status::Ready)
                    processors[i]->work();
                else if (status == IProcessor::Status::Finished)
                    finished[i] = true;
            }
            if (done())
                return true;
        }
        return false;
    }

    void addTo(Processors & processors)
    {
        processors.insert(processors.end(), {partition, coalescing, publisher});
    }

    void cancel(IProcessor::CancelReason reason = IProcessor::CancelReason::Unknown)
    {
        partition->cancel(reason);
        coalescing->cancel(reason);
        publisher->cancel(reason);
    }
};

struct Publication
{
    AdaptiveAggregationPublishTransform processor;
    OutputPort producer;
    InputPort completion;

    Publication(const AggregatingTransformParamsPtr & params, const AdaptiveAggregationSessionPtr & session)
        : processor(params, session), producer(params->aggregator.getAdaptiveStagedHeader()), completion(Block())
    {
        connect(producer, processor.getInputs().front());
        connect(processor.getOutputs().front(), completion);
    }
};

Chunk keyRange(UInt64 begin, size_t rows)
{
    auto column = ColumnUInt64::create();
    auto & values = column->getData();
    values.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        values[i] = begin + i;
    return Chunk(Columns{std::move(column)}, rows);
}

Chunk recordedBlock(const AggregatingTransformParamsPtr & params, size_t rows, bool own_tracker = false)
{
    auto chunk = keyRange(0, rows);
    /// A count-only aggregation records runs with multiplicities, as the frozen kernel does.
    const bool counts_only = params->aggregator.hasAdaptiveCountPayload();
    auto misses = std::make_shared<AdaptiveAggregationMissesInfo>(own_tracker);
    misses->beginRecording(AdaptiveAggregationMissesInfo::KeyBytes::Fixed, sizeof(UInt64), /*constant_key=*/false, counts_only, 0, 0);
    for (UInt32 row = 0; row < rows; ++row)
    {
        if (counts_only)
            misses->recordCountRun<UInt64, false>(row, 0, 0, UInt64(row), 1);
        else
            misses->recordMiss<UInt64, false>(row, 0, 0, UInt64(row));
    }
    chunk.getChunkInfos().add(std::move(misses));
    params->aggregator.extractAdaptiveArguments(chunk, /*key_column=*/nullptr);
    return chunk;
}

Chunk partitionedBlock(
    const AggregatingTransformParamsPtr & params, const AdaptiveAggregationSessionPtr & session, size_t rows = 1)
{
    StagedChunkConverter converter;
    return params->aggregator.partitionAdaptiveBlock(*session, converter, recordedBlock(params, rows));
}

void acknowledgeBlock(AdaptiveAggregatingTransform & producer, StagingPipeline & staging)
{
    ASSERT_EQ(producer.prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(staging.runUntil([&] { return producer.getOutputs().front().canPush(); }));
    ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
    producer.work();
}

class KeySink final : public ISink
{
public:
    explicit KeySink(SharedHeader header) : ISink(std::move(header)) {}
    String getName() const override { return "KeySink"; }
    size_t rows = 0;
    UInt64 sum = 0;

private:
    void consume(Chunk chunk) override
    {
        rows += chunk.getNumRows();
        for (const auto value : assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front()).getData())
            sum += value;
    }
};

/// Runs work on another thread and checks that its allocation context is restored before returning.
void onWorker(MemoryTracker & parent, const std::function<void()> & work)
{
    std::async(std::launch::async, [&]
    {
        ThreadStatus thread_status;
        MemoryTrackerSwitcher switcher(&parent);
        work();
        EXPECT_EQ(CurrentThread::getMemoryTracker()->getParent(), &parent);
    }).get();
}

struct BlockExecution
{
    explicit BlockExecution(AggregatingTransformParamsPtr params_)
        : params(std::move(params_))
        , session(std::make_shared<AdaptiveAggregationSession>())
        , adaptive(session)
        , execution(adaptive)
        , key_columns(params->params.keys_size)
        , aggregate_columns(params->params.aggregates_size)
        , staging(params, session)
        , source(params->aggregator.getAdaptiveArgumentHeader())
        , completion(Block())
    {
        connect(source, staging.input());
        connect(staging.output(), completion);
        staging.prime();
    }

    bool execute(Chunk chunk)
    {
        input = std::move(chunk);
        const bool result_value = params->aggregator.executeOnBlock(
            input.getColumns(), 0, input.getNumRows(), result, key_columns, aggregate_columns, no_more_keys, &execution);
        /// Only a forwarded block carries a recording; every other block has finished its checks.
        if (execution.hasPendingBlock())
        {
            input.getChunkInfos().add(std::move(execution.misses));
            params->aggregator.extractAdaptiveArguments(input, std::move(execution.key_column));
        }
        return result_value;
    }

    void stage(Chunk chunk)
    {
        source.push(std::move(chunk));
        ASSERT_TRUE(staging.runUntil([&] { return source.canPush(); }));
    }

    void admit(MemoryTracker & parent)
    {
        onWorker(parent, [&] { stage(std::move(input)); });
    }

    bool process(Chunk chunk)
    {
        if (!execute(std::move(chunk)))
            return false;
        if (!execution.hasPendingBlock())
            return true;
        stage(std::move(input));
        return params->aggregator.resumeAdaptiveBlock(execution, result, no_more_keys);
    }

    void resume(MemoryTracker & parent)
    {
        onWorker(parent, [&]
        {
            EXPECT_TRUE(params->aggregator.resumeAdaptiveBlock(execution, result, no_more_keys));
        });
    }

    void finish()
    {
        source.finish();
        ASSERT_TRUE(staging.runUntil([&] { return completion.isFinished(); }));
    }

    AggregatingTransformParamsPtr params;
    AdaptiveAggregationSessionPtr session;
    AdaptiveAggregationProducer adaptive;
    AdaptiveAggregationExecution execution;
    AggregatedDataVariants result;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    bool no_more_keys = false;
    Chunk input;
    StagingPipeline staging;
    OutputPort source;
    InputPort completion;
};

}

TEST(AdaptiveAggregationPipeline, ProducerForwardsOnlyAggregateArguments)
{
    for (const String aggregate : {"", "count", "sum"})
    {
        SCOPED_TRACE(aggregate);
        auto header = makeHeader();
        auto params = makeParams(header, 0, aggregate);
        auto many_data = std::make_shared<ManyAggregatedData>(2);
        many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
        AdaptiveAggregatingTransform producer(header, params, many_data, 0);
        OutputPort source(header);
        InputPort partition(params->aggregator.getAdaptiveArgumentHeader());
        connect(source, producer.getInputs().front());
        connect(producer.getOutputs().front(), partition);
        partition.setNeeded();
        ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
        auto chunk = keyRange(0, 8192);
        auto column = chunk.getColumns().front();
        source.push(std::move(chunk));
        ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
        producer.work();
        ASSERT_EQ(producer.prepare(), IProcessor::Status::PortFull);
        chunk = partition.pull(/*set_not_needed=*/true);
        EXPECT_EQ(chunk.getNumRows(), 8192);
        if (aggregate == "sum")
        {
            ASSERT_EQ(chunk.getNumColumns(), 1);
            EXPECT_EQ(chunk.getColumns().front().get(), column.get());
        }
        else
        {
            EXPECT_EQ(chunk.getNumColumns(), 0);
            EXPECT_EQ(column->use_count(), 1);
        }
        const auto misses = chunk.getChunkInfos().get<AdaptiveAggregationMissesInfo>();
        ASSERT_NE(misses, nullptr);
        EXPECT_GT(misses->size(), 0);
        EXPECT_LT(misses->size(), chunk.getNumRows());
        EXPECT_EQ(producer.prepare(), IProcessor::Status::PortFull);
    }
}

TEST(AdaptiveAggregationPipeline, AcknowledgementFollowsIndependentRegistration)
{
    auto params = makeParams(makeHeader());
    auto session = std::make_shared<AdaptiveAggregationSession>();
    Publication first(params, session);
    Publication second(params, session);
    EXPECT_EQ(first.processor.prepare(), IProcessor::Status::NeedData);
    EXPECT_EQ(second.processor.prepare(), IProcessor::Status::NeedData);
    first.producer.push(partitionedBlock(params, session));
    EXPECT_EQ(first.processor.prepare(), IProcessor::Status::Ready);
    EXPECT_FALSE(first.producer.canPush());
    EXPECT_EQ(session->backlog.undrainedRecords(), 0);

    second.producer.push(partitionedBlock(params, session));
    EXPECT_EQ(second.processor.prepare(), IProcessor::Status::Ready);
    second.processor.work();
    EXPECT_FALSE(second.producer.canPush());
    EXPECT_EQ(second.processor.prepare(), IProcessor::Status::NeedData);
    EXPECT_TRUE(second.producer.canPush());
    EXPECT_FALSE(first.producer.canPush());
    EXPECT_EQ(session->backlog.undrainedRecords(), 1);
    EXPECT_EQ(session->backlog.forMergeBucket(0).front().use_count(), 1);

    first.processor.work();
    EXPECT_FALSE(first.producer.canPush());
    EXPECT_EQ(first.processor.prepare(), IProcessor::Status::NeedData);
    EXPECT_EQ(session->backlog.undrainedRecords(), 2);
    EXPECT_FALSE(first.completion.hasData());
    EXPECT_FALSE(second.completion.hasData());
    EXPECT_FALSE(first.completion.isFinished());
    EXPECT_FALSE(second.completion.isFinished());
}

TEST(AdaptiveAggregationPipeline, StagingRequiresKeyMetadata)
{
    auto params = makeParams(makeHeader());
    auto session = std::make_shared<AdaptiveAggregationSession>();
    StagedChunkConverter converter;
    EXPECT_THROW(params->aggregator.partitionAdaptiveBlock(*session, converter, Chunk(Columns{}, 1)), Exception);
    Publication publication(params, session);
    ASSERT_EQ(publication.processor.prepare(), IProcessor::Status::NeedData);
    publication.producer.push(Chunk(Columns{}, 1));
    ASSERT_EQ(publication.processor.prepare(), IProcessor::Status::Ready);
    EXPECT_THROW(publication.processor.work(), Exception);
    EXPECT_EQ(session->backlog.undrainedRecords(), 0);
    EXPECT_FALSE(publication.producer.canPush());
}

TEST(AdaptiveAggregationPipeline, CompletionWaitsForPulledPayload)
{
    auto header = makeHeader();
    auto params = makeParams(header);
    auto many_data = std::make_shared<ManyAggregatedData>(2);
    many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
    AdaptiveAggregationMergeTransform merge(params, many_data, 2, 2, nullptr);
    AdaptiveAggregationPublishTransform first(params, many_data->adaptive_session);
    AdaptiveAggregationPublishTransform second(params, many_data->adaptive_session);
    OutputPort first_producer(params->aggregator.getAdaptiveStagedHeader());
    OutputPort second_producer(params->aggregator.getAdaptiveStagedHeader());
    InputPort result(header);
    connect(first_producer, first.getInputs().front());
    connect(second_producer, second.getInputs().front());
    auto completion = merge.getInputs().begin();
    connect(first.getOutputs().front(), *completion++);
    connect(second.getOutputs().front(), *completion);
    connect(merge.getOutputs().front(), result);

    EXPECT_EQ(merge.prepare({&merge.getInputs().front(), &merge.getInputs().back()}, {}), IProcessor::Status::NeedData);
    EXPECT_EQ(second.prepare(), IProcessor::Status::NeedData);
    second_producer.push(partitionedBlock(params, many_data->adaptive_session));
    second_producer.finish();
    EXPECT_EQ(second.prepare(), IProcessor::Status::Ready);
    first_producer.finish();
    EXPECT_EQ(first.prepare(), IProcessor::Status::Finished);
    EXPECT_EQ(merge.prepare({&merge.getInputs().front(), &merge.getInputs().back()}, {}), IProcessor::Status::NeedData);
    second.work();
    EXPECT_EQ(second.prepare(), IProcessor::Status::Finished);
    EXPECT_EQ(merge.prepare({&merge.getInputs().front(), &merge.getInputs().back()}, {}), IProcessor::Status::Ready);
    EXPECT_EQ(many_data->adaptive_session->backlog.undrainedRecords(), 1);
}

TEST(AdaptiveAggregationPipeline, CancellationReleasesQueuedAndPulledPayloads)
{
    for (const bool pulled : {false, true})
    {
        for (const bool cancelled : {false, true})
        {
            SCOPED_TRACE(pulled);
            SCOPED_TRACE(cancelled);
            auto params = makeParams(makeHeader());
            auto session = std::make_shared<AdaptiveAggregationSession>();
            Publication publication(params, session);
            ASSERT_EQ(publication.processor.prepare(), IProcessor::Status::NeedData);
            auto chunk = partitionedBlock(params, session);
            std::weak_ptr<const StagedKeysInfo> weak = chunk.getChunkInfos().get<StagedKeysInfo>();
            publication.producer.push(std::move(chunk));
            if (pulled)
                ASSERT_EQ(publication.processor.prepare(), IProcessor::Status::Ready);
            EXPECT_FALSE(weak.expired());
            if (cancelled)
                publication.processor.cancel();
            else
                publication.completion.close();
            EXPECT_EQ(publication.processor.prepare(), IProcessor::Status::Finished);
            EXPECT_TRUE(weak.expired());
            EXPECT_TRUE(session->cancelled.load());
            EXPECT_TRUE(publication.producer.isFinished());
            EXPECT_FALSE(publication.processor.getInputs().front().hasData());
            EXPECT_EQ(session->backlog.undrainedRecords(), 0);
        }
    }
}

TEST(AdaptiveAggregationPipeline, CompletesAtSingleAndMultipleExecutorThreads)
{
    MainThreadStatus::getInstance();
    for (const size_t threads : {1, 4})
    {
        for (const size_t external_threshold : {0, 1})
        {
            SCOPED_TRACE(threads);
            SCOPED_TRACE(external_threshold);
            auto group = ThreadGroup::createForQuery(getContext().context);
            ThreadGroupSwitcher group_switcher(group, ThreadName::UNKNOWN, /*allow_existing_group=*/true);
            auto header = makeHeader();
            auto params = makeParams(header, external_threshold);
            auto many_data = std::make_shared<ManyAggregatedData>(2);
            many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
            auto merge = std::make_shared<AdaptiveAggregationMergeTransform>(params, many_data, 2, 2, nullptr);
            auto processors = std::make_shared<Processors>();
            auto completion = merge->getInputs().begin();
            for (size_t producer_index = 0; producer_index < 2; ++producer_index)
            {
                const UInt64 begin = producer_index * 70000;
                Chunks chunks;
                /// A small candidate remains buffered when the next candidate passes through.
                /// Final flushing must deliver that remaining candidate before merge assembly.
                chunks.push_back(keyRange(begin, 8192));
                chunks.push_back(keyRange(begin + 8192, 140000));
                auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
                auto producer = std::make_shared<AdaptiveAggregatingTransform>(
                    header, params, many_data, producer_index);
                StagingPipeline staging(params, many_data->adaptive_session);
                connect(source->getPort(), producer->getInputs().front());
                connect(producer->getOutputs().front(), staging.input());
                connect(staging.output(), *completion++);
                processors->insert(processors->end(), {source, producer});
                staging.addTo(*processors);
            }
            auto sink = std::make_shared<KeySink>(header);
            connect(merge->getOutputs().front(), sink->getPort());
            processors->insert(processors->end(), {merge, sink});
            PipelineExecutor executor(processors, QueryStatusPtr{});
            executor.execute(threads, false);
            constexpr UInt64 expected_rows = 70000 + 8192 + 140000;
            EXPECT_EQ(sink->rows, expected_rows);
            EXPECT_EQ(sink->sum, expected_rows * (expected_rows - 1) / 2);
            EXPECT_EQ(group->performance_counters[ProfileEvents::AdaptiveAggregationPressureDrainedRecords] > 0, external_threshold != 0);
        }
    }
}

TEST(AdaptiveAggregationPipeline, CompletionUsesUpdatedPortsAndCountsEachClosureOnce)
{
    auto header = makeHeader();
    auto params = makeParams(header);
    auto many_data = std::make_shared<ManyAggregatedData>(3);
    many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
    AdaptiveAggregationMergeTransform merge(params, many_data, 3, 3, nullptr);
    OutputPort first{Block()};
    OutputPort second{Block()};
    OutputPort third{Block()};
    InputPort result(header);
    auto input = merge.getInputs().begin();
    auto * first_input = &*input++;
    auto * second_input = &*input++;
    auto * third_input = &*input;
    connect(first, *first_input);
    connect(second, *second_input);
    connect(third, *third_input);
    connect(merge.getOutputs().front(), result);

    first.finish();
    EXPECT_EQ(merge.prepare({}, {}), IProcessor::Status::NeedData);
    second.finish();
    EXPECT_EQ(merge.prepare({first_input, second_input, second_input}, {}), IProcessor::Status::NeedData);
    EXPECT_EQ(merge.prepare({second_input}, {}), IProcessor::Status::NeedData);
    third.finish();
    EXPECT_EQ(merge.prepare({third_input}, {}), IProcessor::Status::Ready);
}

TEST(AdaptiveAggregationPipeline, AcknowledgementReleasesInputBeforePostBlockChecks)
{
    MainThreadStatus::getInstance();
    for (const String aggregate : {"", "count", "sum"})
    {
        for (const bool thaw : {false, true})
        {
            for (const bool own_tracker : {false, true})
            {
                SCOPED_TRACE(aggregate);
                SCOPED_TRACE(thaw);
                SCOPED_TRACE(own_tracker);
                MemoryTracker query_tracker(nullptr, VariableContext::Process, false);
                MemoryTrackerSwitcher query_scope(&query_tracker);
                auto header = makeHeader();
                auto aggregation_params = makeParams(header, 64 << 20, aggregate)->params;
                aggregation_params.group_by_two_level_threshold_bytes = 192 << 20;
                auto params = std::make_shared<AggregatingTransformParams>(header, aggregation_params, true);
                MemoryTracker nested_tracker(&query_tracker, VariableContext::Thread, false);
                auto & parent = own_tracker ? query_tracker : nested_tracker;
                MemoryTrackerSwitcher execution_scope(&parent);
                BlockExecution block(params);

                /// Coalescing buffers the first candidate until a later block triggers pressure.
                ASSERT_TRUE(block.process(keyRange(0, 8192)));
                ASSERT_TRUE(block.adaptive.isFrozen());
                ASSERT_FALSE(block.execution.hasPendingBlock());
                auto input = keyRange(8192, 150000);
                auto owner = input.getColumns().front();
                ASSERT_TRUE(block.execute(std::move(input)));
                ASSERT_TRUE(block.execution.hasPendingBlock());
                EXPECT_EQ(block.input.getChunkInfos().get<AdaptiveAggregationMissesInfo>()->use_own_memory_tracker, own_tracker);
                EXPECT_EQ(owner->use_count() > 1, aggregate == "sum");
                EXPECT_EQ(CurrentThread::getMemoryTracker()->getParent(), &parent);

                /// Both the buffered candidate and the current block are published before acknowledgement.
                constexpr Int64 pressure_bytes = 128 << 20;
                query_tracker.adjustWithUntrackedMemory(pressure_bytes);
                SCOPE_EXIT({ query_tracker.adjustWithUntrackedMemory(-pressure_bytes); });
                block.admit(parent);
                EXPECT_EQ(owner->use_count(), 1);
                EXPECT_EQ(block.session->backlog.undrainedRecords(), 154096);
                block.session->thaw_all.store(thaw);
                block.resume(parent);
                EXPECT_EQ(block.adaptive.isBaseline(), thaw);
                EXPECT_FALSE(block.execution.hasPendingBlock());
                EXPECT_FALSE(block.result.isTwoLevel());
                EXPECT_EQ(block.session->backlog.undrainedRecords(), 0);
                EXPECT_EQ(block.result.size() + block.session->early_drain_variants->size(), 158192);
                EXPECT_FALSE(params->aggregator.hasTemporaryData());
            }
        }
    }
}

TEST(AdaptiveAggregationPipeline, AllHitBlockFlushesBufferedMissesBeforeAcknowledgement)
{
    MainThreadStatus::getInstance();
    for (const String aggregate : {"", "count", "sum"})
    {
        SCOPED_TRACE(aggregate);
        MemoryTracker query_tracker(nullptr, VariableContext::Process, false);
        MemoryTrackerSwitcher query_scope(&query_tracker);
        auto params = makeParams(makeHeader(), 64 << 20, aggregate);
        BlockExecution block(params);
        ASSERT_TRUE(block.process(keyRange(0, 8192)));
        ASSERT_TRUE(block.adaptive.isFrozen());
        EXPECT_EQ(block.session->backlog.undrainedRecords(), 0);

        /// All rows hit the frozen table, but the preceding block still has buffered misses. Under
        /// memory pressure the block is forwarded anyway: its empty partitioned chunk must reach the
        /// coalescer and trigger the pressure flush before the drain.
        constexpr Int64 pressure_bytes = 128 << 20;
        query_tracker.adjustWithUntrackedMemory(pressure_bytes);
        SCOPE_EXIT({ query_tracker.adjustWithUntrackedMemory(-pressure_bytes); });
        ASSERT_TRUE(block.execute(keyRange(0, 32)));
        ASSERT_TRUE(block.execution.hasPendingBlock());
        ASSERT_EQ(block.input.getChunkInfos().get<AdaptiveAggregationMissesInfo>()->size(), 0);
        block.admit(query_tracker);
        EXPECT_EQ(block.session->backlog.undrainedRecords(), 4096);
        block.resume(query_tracker);
        EXPECT_EQ(block.session->backlog.undrainedRecords(), 0);
        EXPECT_EQ(block.result.size() + block.session->early_drain_variants->size(), 8192);
        EXPECT_FALSE(block.execution.hasPendingBlock());
    }
}

TEST(AdaptiveAggregationPipeline, FrozenConstantMissesPreserveCountsAndArguments)
{
    MainThreadStatus::getInstance();
    for (const bool string_keys : {false, true})
    {
        for (const String aggregate : {"", "count", "max"})
        {
            SCOPED_TRACE(string_keys);
            SCOPED_TRACE(aggregate);
            DataTypePtr key_type = string_keys
                ? DataTypePtr(std::make_shared<DataTypeString>()) : std::make_shared<DataTypeUInt64>();
            auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(key_type, "key")});
            auto aggregation_params = makeParams(header, 0, aggregate)->params;
            aggregation_params.optimize_group_by_constant_keys = true;
            auto params = std::make_shared<AggregatingTransformParams>(header, aggregation_params, true);
            BlockExecution block(params);
            const auto key = [&](UInt64 value) { return string_keys ? Field(std::to_string(value)) : Field(value); };
            std::map<Field, Field> expected;
            auto learning_keys = key_type->createColumn();
            for (UInt64 i = 0; i < 64; ++i)
            {
                learning_keys->insert(key(i));
                expected.emplace(key(i), aggregate == "max" ? key(i) : Field(UInt64(1)));
            }
            ASSERT_TRUE(block.process(Chunk(Columns{std::move(learning_keys)}, 64)));
            ASSERT_TRUE(block.adaptive.isFrozen());

            /// New constant keys must miss the frozen table. Repeated blocks exercise coalescing,
            /// while the constant's one stored row must supply every staged argument row.
            for (const size_t rows : {17, 23})
            {
                auto column = key_type->createColumnConst(rows, key(1000));
                ASSERT_TRUE(block.process(Chunk(Columns{std::move(column)}, rows)));
                ASSERT_FALSE(block.execution.hasPendingBlock());
            }
            expected.emplace(key(1000), aggregate == "max" ? key(1000) : Field(UInt64(aggregate == "count" ? 40 : 1)));
            block.finish();
            /// Count sampling retains both block contributions before coalescing merges their equal keys.
            if (aggregate == "count")
                EXPECT_EQ(block.session->staged_records, 2);

            block.result.convertToTwoLevel();
            for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
                params->aggregator.drainAdaptiveBucketForMerge(
                    block.result, block.result.aggregates_pool, bucket, *block.session, block.session->cancelled);
            std::map<Field, Field> actual;
            for (const auto & result : params->aggregator.convertToChunks(block.result, true))
            {
                const auto & columns = result.chunk.getColumns();
                for (size_t row = 0; row < result.chunk.getNumRows(); ++row)
                    EXPECT_TRUE(actual.emplace((*columns[0])[row], aggregate.empty() ? Field(UInt64(1)) : (*columns[1])[row]).second);
            }
            EXPECT_EQ(actual, expected);
        }
    }
}

TEST(AdaptiveAggregationPipeline, ProducerWaitsForEveryPublicationBeforePressureAndCompletion)
{
    MainThreadStatus::getInstance();
    MemoryTracker query_tracker(nullptr, VariableContext::Process, false);
    MemoryTrackerSwitcher query_scope(&query_tracker);
    auto header = makeHeader();
    auto params = makeParams(header, 64 << 20, "sum");
    auto many_data = std::make_shared<ManyAggregatedData>(2);
    auto session = std::make_shared<AdaptiveAggregationSession>();
    many_data->adaptive_session = session;
    AdaptiveAggregatingTransform producer(header, params, many_data, 0);
    StagingPipeline staging(params, session);
    OutputPort source(header);
    InputPort completion{Block()};
    connect(source, producer.getInputs().front());
    connect(producer.getOutputs().front(), staging.input());
    connect(staging.output(), completion);
    staging.prime();
    ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
    source.push(keyRange(0, 8192));
    ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
    producer.work();
    acknowledgeBlock(producer, staging);
    ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
    auto input = keyRange(8192, 150000);
    auto owner = input.getColumns().front();
    source.push(std::move(input));
    source.finish();
    ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
    producer.work();

    constexpr Int64 pressure_bytes = 128 << 20;
    query_tracker.adjustWithUntrackedMemory(pressure_bytes);
    SCOPE_EXIT({ query_tracker.adjustWithUntrackedMemory(-pressure_bytes); });
    ASSERT_EQ(producer.prepare(), IProcessor::Status::PortFull);
    EXPECT_GT(owner->use_count(), 1);
    ASSERT_EQ(staging.partition->prepare(), IProcessor::Status::Ready);
    onWorker(query_tracker, [&] { staging.partition->work(); });
    EXPECT_EQ(owner->use_count(), 1);
    EXPECT_EQ(producer.prepare(), IProcessor::Status::PortFull);
    ASSERT_EQ(staging.partition->prepare(), IProcessor::Status::PortFull);
    ASSERT_EQ(staging.coalescing->prepare(), IProcessor::Status::Ready);
    onWorker(query_tracker, [&] { staging.coalescing->work(); });

    /// A large candidate passes through before the earlier small candidate is flushed under pressure.
    for (const size_t admitted_records : {150000, 154096})
    {
        ASSERT_EQ(staging.coalescing->prepare(), IProcessor::Status::PortFull);
        ASSERT_EQ(staging.publisher->prepare(), IProcessor::Status::Ready);
        EXPECT_EQ(producer.prepare(), IProcessor::Status::PortFull);
        onWorker(query_tracker, [&] { staging.publisher->work(); });
        EXPECT_EQ(session->backlog.undrainedRecords(), admitted_records);
        EXPECT_EQ(producer.prepare(), IProcessor::Status::PortFull);
        EXPECT_FALSE(session->early_drain_variants->hasData());
        EXPECT_FALSE(completion.isFinished());
        ASSERT_EQ(staging.publisher->prepare(), IProcessor::Status::NeedData);
        if (admitted_records == 150000)
        {
            ASSERT_EQ(staging.coalescing->prepare(), IProcessor::Status::Ready);
            onWorker(query_tracker, [&] { staging.coalescing->work(); });
        }
    }
    ASSERT_EQ(staging.coalescing->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(staging.partition->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
    onWorker(query_tracker, [&] { producer.work(); });
    EXPECT_EQ(session->backlog.undrainedRecords(), 0);
    EXPECT_EQ(session->early_drain_variants->size(), 154096);
    ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
    producer.work();
    ASSERT_EQ(producer.prepare(), IProcessor::Status::Finished);
    ASSERT_TRUE(staging.runUntil([&] { return completion.isFinished(); }));
}

TEST(AdaptiveAggregationPipeline, StagingUsesThePublishingAllocationContext)
{
    MainThreadStatus::getInstance();
    for (const bool own_tracker : {false, true})
    {
        SCOPED_TRACE(own_tracker);
        MemoryTracker query_tracker(nullptr, VariableContext::Process, false);
        MemoryTrackerSwitcher query_scope(&query_tracker);
        auto header = makeHeader();
        auto aggregation_params = makeParams(header, 0, "count")->params;
        aggregation_params.group_by_two_level_threshold_bytes = 64 << 10;
        auto params = std::make_shared<AggregatingTransformParams>(header, aggregation_params, true);
        BlockExecution block(params);
        block.adaptive.standDown(AdaptiveAggregationProducer::BaselineState::Reason::TooFewDistinctKeys);
        ASSERT_TRUE(block.process(keyRange(0, 64)));
        ASSERT_FALSE(block.result.isTwoLevel());
        auto chunk = recordedBlock(params, 8192, own_tracker);
        onWorker(query_tracker, [&]
        {
            /// Staging contributes to the aggregation account only when requested by the producer.
            block.stage(std::move(chunk));
        });
        ASSERT_TRUE(block.process(keyRange(0, 0)));
        EXPECT_EQ(block.result.isTwoLevel(), own_tracker);
    }
}

TEST(AdaptiveAggregationPipeline, CancellationReleasesUnsentAndQueuedInput)
{
    for (const bool published : {false, true})
    {
        SCOPED_TRACE(published);
        auto header = makeHeader();
        auto params = makeParams(header, 0, "sum");
        auto many_data = std::make_shared<ManyAggregatedData>(2);
        auto session = std::make_shared<AdaptiveAggregationSession>();
        many_data->adaptive_session = session;
        AdaptiveAggregatingTransform producer(header, params, many_data, 0);
        StagingPipeline admission(params, session);
        OutputPort source(header);
        InputPort completion{Block()};
        connect(source, producer.getInputs().front());
        connect(producer.getOutputs().front(), admission.input());
        connect(admission.output(), completion);
        admission.prime();
        ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
        auto input = keyRange(0, 150000);
        auto owner = input.getColumns().front();
        source.push(std::move(input));
        ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
        producer.work();
        EXPECT_GT(owner->use_count(), 1);
        if (published)
            ASSERT_EQ(producer.prepare(), IProcessor::Status::PortFull);
        completion.close();
        ASSERT_TRUE(admission.runUntil([&] { return producer.getOutputs().front().isFinished(); }));
        ASSERT_EQ(producer.prepare(), IProcessor::Status::Finished);
        EXPECT_EQ(owner->use_count(), 1);
        EXPECT_TRUE(source.isFinished());
        EXPECT_EQ(session->backlog.undrainedRecords(), 0);
    }
}

#if USE_LIBFIU
TEST(AdaptiveAggregationPipeline, CancellationWakesPressureWorkWaitingForSpillBudget)
{
    MainThreadStatus::getInstance();
    MemoryTracker query_tracker(nullptr, VariableContext::Process, false);
    MemoryTrackerSwitcher query_scope(&query_tracker);
    auto header = makeHeader();
    auto params = makeParams(header, 64 << 20);
    BlockExecution block(params);
    ASSERT_TRUE(block.process(keyRange(0, 8192)));
    ASSERT_TRUE(block.execute(keyRange(8192, 700000)));
    constexpr Int64 pressure_bytes = 128 << 20;
    query_tracker.adjustWithUntrackedMemory(pressure_bytes);
    SCOPE_EXIT({ query_tracker.adjustWithUntrackedMemory(-pressure_bytes); });
    block.admit(query_tracker);
    ASSERT_EQ(block.session->backlog.undrainedRecords(), 704096);

    AdaptiveAggregationSession::SpillReservation writer;
    ASSERT_TRUE(writer.reserveOrWait(*block.session, pressure_bytes, pressure_bytes));
    Publication admission(params, block.session);
    FailPointInjection::enableFailPoint(FailPoints::adaptive_aggregation_before_spill_budget_wait);
    auto worker = std::async(std::launch::async, [&]
    {
        ThreadStatus thread_status;
        MemoryTrackerSwitcher switcher(&query_tracker);
        return params->aggregator.resumeAdaptiveBlock(block.execution, block.result, block.no_more_keys);
    });
    SCOPE_EXIT({
        FailPointInjection::disableFailPoint(FailPoints::adaptive_aggregation_before_spill_budget_wait);
        block.session->cancel();
    });
    FailPointInjection::waitForPause(FailPoints::adaptive_aggregation_before_spill_budget_wait);
    FailPointInjection::notifyFailPoint(FailPoints::adaptive_aggregation_before_spill_budget_wait);
    {
        /// The predicate holds this mutex at the failpoint. Acquiring it after resumption proves
        /// the worker entered the condition-variable wait while the other writer still owns the budget.
        std::lock_guard lock(block.session->detached_spill_mutex);
        EXPECT_EQ(block.session->estimated_detached_spill_bytes, pressure_bytes);
    }
    admission.processor.cancel();
    const auto status = worker.wait_for(std::chrono::seconds(10));
    EXPECT_EQ(status, std::future_status::ready);
    /// Release the other writer even on failure so a missing cancellation notification cannot hang teardown.
    writer.release();
    EXPECT_TRUE(worker.get());
    EXPECT_TRUE(block.session->cancelled.load());
    EXPECT_FALSE(block.execution.hasPendingBlock());
    EXPECT_FALSE(params->aggregator.hasTemporaryData());
    EXPECT_EQ(block.session->estimated_detached_spill_bytes, 0);
}
#endif

TEST(AdaptiveAggregationPipeline, FinalAssemblyIncludesLateSpillsAndReadersOwnTemporaryFiles)
{
    MainThreadStatus::getInstance();
    for (const bool late_producer_spill : {false, true})
    {
        SCOPED_TRACE(late_producer_spill);
        auto disk = createDisk("adaptive_aggregation_final_merge");
        SCOPE_EXIT({ destroyDisk(disk); });
        auto volume = std::make_shared<SingleDiskVolume>("volume", disk);
        auto tmp_data = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, volume);
        MemoryTracker query_tracker(nullptr, VariableContext::Process, false);
        MemoryTrackerSwitcher query_scope(&query_tracker);
        constexpr size_t num_producers = 9;
        constexpr size_t rows_per_producer = 125000;
        constexpr size_t external_threshold = 64 << 20;
        auto header = makeHeader();
        auto params = makeParams(header, external_threshold, {}, tmp_data);
        auto many_data = std::make_shared<ManyAggregatedData>(num_producers);
        auto session = std::make_shared<AdaptiveAggregationSession>();
        many_data->adaptive_session = session;
        auto merge = std::make_unique<AdaptiveAggregationMergeTransform>(params, many_data, 2, 2, nullptr);
        InputPort result(header);
        connect(merge->getOutputs().front(), result);
        std::vector<std::unique_ptr<AdaptiveAggregatingTransform>> producers;
        std::vector<std::unique_ptr<StagingPipeline>> admissions;
        std::vector<std::unique_ptr<OutputPort>> sources;
        IProcessor::UpdatedInputPorts completion_inputs;
        auto completion = merge->getInputs().begin();
        for (size_t i = 0; i < num_producers; ++i)
        {
            auto producer = std::make_unique<AdaptiveAggregatingTransform>(header, params, many_data, i);
            auto admission = std::make_unique<StagingPipeline>(params, session);
            auto source = std::make_unique<OutputPort>(header);
            connect(*source, producer->getInputs().front());
            connect(producer->getOutputs().front(), admission->input());
            connect(admission->output(), *completion);
            completion_inputs.push_back(&*completion++);
            admission->prime();
            ASSERT_EQ(producer->prepare(), IProcessor::Status::NeedData);
            source->push(keyRange(i * rows_per_producer, rows_per_producer));
            ASSERT_EQ(producer->prepare(), IProcessor::Status::Ready);
            producer->work();
            acknowledgeBlock(*producer, *admission);
            ASSERT_EQ(producer->prepare(), IProcessor::Status::NeedData);
            sources.push_back(std::move(source));
            producers.push_back(std::move(producer));
            admissions.push_back(std::move(admission));
        }
        ASSERT_EQ(merge->prepare({}, {}), IProcessor::Status::NeedData);
        Int64 pressure_adjustment = 0;
        SCOPE_EXIT({ query_tracker.adjustWithUntrackedMemory(-pressure_adjustment); });
        for (size_t i = 0; i < num_producers; ++i)
        {
            SCOPED_TRACE(i);
            if (late_producer_spill && i == 1)
            {
                /// The first producer has finished with a resident table when the next producer spills.
                ASSERT_TRUE(many_data->variants.front()->hasData());
                many_data->variants[i]->convertToTwoLevel();
                params->aggregator.writeToTemporaryFile(*many_data->variants[i]);
            }
            sources[i]->finish();
            ASSERT_EQ(producers[i]->prepare(), IProcessor::Status::Ready);
            producers[i]->work();
            ASSERT_EQ(producers[i]->prepare(), IProcessor::Status::Finished);
            ASSERT_TRUE(admissions[i]->runUntil([&] { return admissions[i]->publisher->getInputs().front().hasData(); }));
            ASSERT_EQ(admissions[i]->publisher->prepare(), IProcessor::Status::Ready);
            if (!late_producer_spill && i + 1 == num_producers)
            {
                /// Registration of the ninth final flush grows the per-bucket backlog vectors.
                /// Put the query just below its threshold so that this admission crosses it.
                CurrentThread::flushUntrackedMemory();
                pressure_adjustment = external_threshold - 1 - getCurrentQueryMemoryUsage();
                query_tracker.adjustWithUntrackedMemory(pressure_adjustment);
            }
            admissions[i]->publisher->work();
            CurrentThread::flushUntrackedMemory();
            if (!late_producer_spill && i + 1 == num_producers)
                ASSERT_GT(getCurrentQueryMemoryUsage(), external_threshold);
            ASSERT_TRUE(admissions[i]->runUntil([&] { return completion_inputs[i]->isFinished(); }));
            EXPECT_EQ(merge->prepare({completion_inputs[i]}, {}), i + 1 == num_producers
                ? IProcessor::Status::Ready : IProcessor::Status::NeedData);
        }
        ASSERT_EQ(params->aggregator.hasTemporaryData(), late_producer_spill);
        merge->work();
        ASSERT_EQ(merge->prepare({}, {}), IProcessor::Status::UpdatePipeline);
        ASSERT_GT(tmp_data->currentCompressedSize(), 0);
        EXPECT_FALSE(params->aggregator.hasTemporaryData());
        EXPECT_EQ(session->backlog.undrainedRecords(), 0);

        /// The reader pipeline owns its files independently of the completion coordinator.
        auto update = merge->updatePipeline();
        auto & output = update.to_add.back()->getOutputs().front();
        disconnect(output, merge->getInputs().back());
        disconnect(merge->getOutputs().front(), result);
        auto completion_port = merge->getInputs().begin();
        for (auto & admission : admissions)
            disconnect(admission->output(), *completion_port++);
        merge.reset();
        ASSERT_GT(tmp_data->currentCompressedSize(), 0);
        auto sink = std::make_shared<KeySink>(header);
        connect(output, sink->getPort());
        auto processors = std::make_shared<Processors>(std::move(update.to_add));
        processors->push_back(sink);
        {
            PipelineExecutor executor(processors, QueryStatusPtr{});
            executor.execute(1, false);
        }
        constexpr UInt64 expected_rows = num_producers * rows_per_producer;
        EXPECT_EQ(sink->rows, expected_rows);
        EXPECT_EQ(sink->sum, expected_rows * (expected_rows - 1) / 2);
        EXPECT_GT(tmp_data->currentCompressedSize(), 0);
        processors.reset();
        EXPECT_EQ(tmp_data->currentCompressedSize(), 0);
    }
}

TEST(AdaptiveAggregationPipeline, PartialResultCompletesExistingAndDeferredSpillReaders)
{
    MainThreadStatus::getInstance();
    for (const bool deferred_assembly : {false, true})
    {
        SCOPED_TRACE(deferred_assembly);
        auto disk = createDisk("adaptive_partial_result_spill");
        SCOPE_EXIT({ destroyDisk(disk); });
        auto volume = std::make_shared<SingleDiskVolume>("volume", disk);
        auto tmp_data = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, volume);
        auto header = makeHeader();
        auto params = makeParams(header, 0, {}, tmp_data);
        auto many_data = std::make_shared<ManyAggregatedData>(2);
        if (deferred_assembly)
            many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
        constexpr size_t rows_per_producer = 10000;
        for (size_t i = 0; i < many_data->num_producers; ++i)
        {
            auto chunk = keyRange(i * rows_per_producer, rows_per_producer);
            auto & variant = *many_data->variants[i];
            ColumnRawPtrs key_columns(params->params.keys_size);
            Aggregator::AggregateColumns aggregate_columns(params->params.aggregates_size);
            bool no_more_keys = false;
            ASSERT_TRUE(params->aggregator.executeOnBlock(
                chunk.detachColumns(), 0, rows_per_producer, variant, key_columns, aggregate_columns, no_more_keys, nullptr));
            variant.convertToTwoLevel();
            params->aggregator.writeToTemporaryFile(variant);
        }
        ASSERT_TRUE(params->aggregator.hasTemporaryData());

        auto processors = std::make_shared<Processors>();
        if (deferred_assembly)
        {
            auto merge = std::make_shared<AdaptiveAggregationMergeTransform>(params, many_data, 2, 2, nullptr);
            for (auto & input : merge->getInputs())
            {
                auto completion = std::make_shared<NullSource>(std::make_shared<const Block>());
                connect(completion->getPort(), input);
                processors->push_back(completion);
            }
            processors->push_back(merge);
        }
        else
        {
            *processors = createAggregationMergePipeline(params, many_data, 2, 2, false, false, nullptr);
        }
        auto sink = std::make_shared<KeySink>(header);
        connect(processors->back()->getOutputs().front(), sink->getPort());
        processors->push_back(sink);
        PipelineExecutor executor(processors, QueryStatusPtr{});
        /// The request applies to existing readers and to readers the coordinator adds later.
        /// Both must drain all data already consumed by aggregation.
        executor.cancelReading();
        executor.execute(1, false);
        constexpr UInt64 expected_rows = 2 * rows_per_producer;
        EXPECT_EQ(sink->rows, expected_rows);
        EXPECT_EQ(sink->sum, expected_rows * (expected_rows - 1) / 2);
        if (deferred_assembly)
            EXPECT_FALSE(many_data->adaptive_session->cancelled.load());
    }
}

TEST(AdaptiveAggregationPipeline, CancellationReleasesQueuedAndPulledProducerInput)
{
    for (const bool pulled : {false, true})
    {
        for (const bool cancelled : {false, true})
        {
            SCOPED_TRACE(pulled);
            SCOPED_TRACE(cancelled);
            auto header = makeHeader();
            auto params = makeParams(header);
            auto many_data = std::make_shared<ManyAggregatedData>(2);
            auto session = std::make_shared<AdaptiveAggregationSession>();
            many_data->adaptive_session = session;
            AdaptiveAggregatingTransform producer(header, params, many_data, 0);
            OutputPort source(header);
            InputPort admission(params->aggregator.getAdaptiveArgumentHeader());
            connect(source, producer.getInputs().front());
            connect(producer.getOutputs().front(), admission);
            admission.setNeeded();
            ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
            auto chunk = keyRange(0, 1024);
            auto owner = chunk.getColumns().front();
            source.push(std::move(chunk));
            if (pulled)
                ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
            if (cancelled)
                producer.cancel();
            else
                admission.close();
            EXPECT_EQ(producer.prepare(), IProcessor::Status::Finished);
            EXPECT_EQ(owner->use_count(), 1);
            EXPECT_FALSE(producer.getInputs().front().hasData());
            EXPECT_TRUE(source.isFinished());
            EXPECT_TRUE(session->cancelled.load());
        }
    }
}

TEST(AdaptiveAggregationPipeline, CancellationReleasesQueuedMergeOutput)
{
    for (const bool cancelled : {false, true})
    {
        SCOPED_TRACE(cancelled);
        auto header = makeHeader();
        auto params = makeParams(header);
        auto many_data = std::make_shared<ManyAggregatedData>(1);
        auto session = std::make_shared<AdaptiveAggregationSession>();
        many_data->adaptive_session = session;
        AdaptiveAggregationMergeTransform merge(params, many_data, 1, 1, nullptr);
        OutputPort completion{Block()};
        InputPort result(header);
        connect(completion, merge.getInputs().front());
        connect(merge.getOutputs().front(), result);
        completion.finish();
        ASSERT_EQ(merge.prepare({}, {}), IProcessor::Status::Ready);
        merge.work();
        auto update = merge.updatePipeline();
        result.setNeeded();
        ASSERT_EQ(merge.prepare({}, {}), IProcessor::Status::NeedData);
        auto chunk = keyRange(0, 1024);
        auto owner = chunk.getColumns().front();
        update.to_add.back()->getOutputs().front().push(std::move(chunk));
        if (cancelled)
            merge.cancel();
        else
            result.close();
        EXPECT_EQ(merge.prepare({}, {}), IProcessor::Status::Finished);
        EXPECT_EQ(owner->use_count(), 1);
        EXPECT_FALSE(merge.getInputs().back().hasData());
        EXPECT_TRUE(session->cancelled.load());
    }
}

TEST(AdaptiveAggregationPipeline, EmptyAndEarlyFinishedProducersBeforeLateEngagement)
{
    MainThreadStatus::getInstance();
    for (const size_t late_rows : {0, 32, 8192})
    {
        for (const bool partial_result : {false, true})
        {
            SCOPED_TRACE(late_rows);
            SCOPED_TRACE(partial_result);
            auto header = makeHeader();
            auto params = makeParams(header);
            auto many_data = std::make_shared<ManyAggregatedData>(3);
            auto session = std::make_shared<AdaptiveAggregationSession>();
            many_data->adaptive_session = session;
            AdaptiveAggregationMergeTransform merge(params, many_data, 2, 2, nullptr);
            InputPort result(header);
            connect(merge.getOutputs().front(), result);
            std::vector<std::unique_ptr<AdaptiveAggregatingTransform>> producers;
            std::vector<std::unique_ptr<StagingPipeline>> admissions;
            std::vector<std::unique_ptr<OutputPort>> sources;
            auto completion = merge.getInputs().begin();
            for (size_t i = 0; i < many_data->num_producers; ++i)
            {
                auto producer = std::make_unique<AdaptiveAggregatingTransform>(header, params, many_data, i);
                auto admission = std::make_unique<StagingPipeline>(params, session);
                auto source = std::make_unique<OutputPort>(header);
                connect(*source, producer->getInputs().front());
                connect(producer->getOutputs().front(), admission->input());
                connect(admission->output(), *completion++);
                producers.push_back(std::move(producer));
                admissions.push_back(std::move(admission));
                sources.push_back(std::move(source));
            }
            ASSERT_EQ(merge.prepare({}, {}), IProcessor::Status::NeedData);
            completion = merge.getInputs().begin();
            for (size_t i = 0; i < many_data->num_producers; ++i, ++completion)
            {
                auto & producer = *producers[i];
                auto & admission = *admissions[i];
                auto & source = *sources[i];
                ASSERT_FALSE(session->initialized.load());
                admission.prime();
                ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
                const size_t rows = i == 0 ? 0 : i == 1 ? std::min<size_t>(32, late_rows) : late_rows;
                if (rows)
                {
                    source.push(keyRange(i == 2 ? 16 : 0, rows));
                    ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
                    producer.work();
                    /// Only a block that froze the table and recorded misses is forwarded; a learning
                    /// producer finishes its block in place.
                    if (rows > params->params.adaptive_aggregator_freeze_threshold)
                        acknowledgeBlock(producer, admission);
                    ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
                }
                if (partial_result)
                {
                    producer.cancel(IProcessor::CancelReason::PartialResult);
                    admission.cancel(IProcessor::CancelReason::PartialResult);
                    merge.cancel(IProcessor::CancelReason::PartialResult);
                }
                source.finish();
                ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
                producer.work();
                ASSERT_EQ(producer.prepare(), IProcessor::Status::Finished);
                /// Coalescing flushes buffered misses before the coordinator can merge the tables.
                ASSERT_TRUE(admission.runUntil([&] { return completion->isFinished(); }));
                EXPECT_EQ(merge.prepare({&*completion}, {}), i == 2 ? IProcessor::Status::Ready : IProcessor::Status::NeedData);
            }
            EXPECT_EQ(session->initialized.load(), late_rows == 8192);
            EXPECT_FALSE(session->cancelled.load());
            merge.work();
            auto update = merge.updatePipeline();
            auto & output = update.to_add.back()->getOutputs().front();
            disconnect(output, merge.getInputs().back());
            auto sink = std::make_shared<KeySink>(header);
            connect(output, sink->getPort());
            auto processors = std::make_shared<Processors>(std::move(update.to_add));
            processors->push_back(sink);
            PipelineExecutor executor(processors, QueryStatusPtr{});
            if (partial_result)
                executor.cancelReading();
            executor.execute(1, false);
            const UInt64 expected_rows = late_rows ? 16 + late_rows : 0;
            EXPECT_EQ(sink->rows, expected_rows);
            EXPECT_EQ(sink->sum, expected_rows ? expected_rows * (expected_rows - 1) / 2 : 0);
        }
    }
}
