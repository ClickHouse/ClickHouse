#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/PromQLNativeTagsBarrierStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int QUERY_WAS_CANCELLED;
}

namespace
{

using Collector = ContextTimeSeriesTagsCollector;
using Tags = Collector::TagNamesAndValues;
using TagsPtr = Collector::TagNamesAndValuesPtr;
using BarrierTransform = PromQLNativeTagsBarrierTransform;

SharedHeader makeUInt64Header(std::string_view name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(type->createColumn(), type, String{name})});
}

Chunk makeUInt64Chunk(std::initializer_list<UInt64> values)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value : values)
        column->insertValue(value);
    const size_t size = column->size();
    return Chunk(Columns{std::move(column)}, size);
}

TagsPtr makeTags(UInt64 id)
{
    auto tags = std::make_shared<Tags>();
    tags->emplace_back("__name__", "up");
    tags->emplace_back("series", std::to_string(id));
    return tags;
}

template <typename Function>
void expectExceptionCode(Function && function, int expected_code)
{
    try
    {
        function();
        FAIL() << "Expected DB::Exception with code " << expected_code;
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << exception.message();
    }
}

struct SourceState
{
    std::atomic_size_t generate_calls{0};
    std::atomic_bool precondition_failed{false};
};

class CountingSource final : public ISource
{
public:
    using BeforeGenerate = std::function<bool()>;

    CountingSource(SharedHeader header_, Chunks chunks_, std::shared_ptr<SourceState> state_, BeforeGenerate before_generate_ = {})
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
        , chunks(std::move(chunks_))
        , state(std::move(state_))
        , before_generate(std::move(before_generate_))
    {
    }

    String getName() const override { return "PromQLNativeBarrierCountingSource"; }

protected:
    Chunk generate() override
    {
        state->generate_calls.fetch_add(1, std::memory_order_relaxed);
        if (before_generate && !before_generate())
            state->precondition_failed.store(true, std::memory_order_relaxed);

        if (next_chunk == chunks.size())
            return {};

        return std::move(chunks[next_chunk++]);
    }

private:
    Chunks chunks;
    std::shared_ptr<SourceState> state;
    BeforeGenerate before_generate;
    size_t next_chunk = 0;
};

struct BlockingSourceState
{
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool released = false;
    std::atomic_size_t generate_calls{0};
};

struct SealGate
{
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool released = false;
};

using ExtractorGate = SealGate;

struct StartCancelGate
{
    std::mutex mutex;
    std::condition_variable cv;
    bool start_entered = false;
    bool start_released = false;
    bool cancellation_transitioned = false;
};

class BlockingSource final : public ISource
{
public:
    BlockingSource(SharedHeader header_, std::shared_ptr<BlockingSourceState> state_)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
        , state(std::move(state_))
    {
    }

    String getName() const override { return "PromQLNativeBarrierBlockingSource"; }

protected:
    Chunk generate() override
    {
        state->generate_calls.fetch_add(1, std::memory_order_relaxed);
        std::unique_lock lock(state->mutex);
        state->entered = true;
        state->cv.notify_all();
        state->cv.wait(lock, [&] { return state->released; });
        return {};
    }

    void onCancel() noexcept override
    {
        {
            std::lock_guard lock(state->mutex);
            state->released = true;
        }
        state->cv.notify_all();
    }

private:
    std::shared_ptr<BlockingSourceState> state;
};

struct BuiltBarrierPipeline
{
    std::unique_ptr<PromQLNativeTagsBarrierStep> step;
    QueryPipeline pipeline;
};

BuiltBarrierPipeline buildBarrierPipeline(
    const SharedHeader & main_header,
    const SourcePtr & main_source,
    const SharedHeader & tags_header,
    const SourcePtr & tags_source,
    const std::shared_ptr<Collector> & collector,
    BarrierTransform::TagsExtractor extractor)
{
    auto main_pipeline = std::make_unique<QueryPipelineBuilder>();
    main_pipeline->init(Pipe(main_source));

    auto tags_pipeline = std::make_unique<QueryPipelineBuilder>();
    tags_pipeline->init(Pipe(tags_source));

    auto step = std::make_unique<PromQLNativeTagsBarrierStep>(
        main_header, tags_header, collector, std::move(extractor));

    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(main_pipeline));
    inputs.emplace_back(std::move(tags_pipeline));

    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step->updatePipeline(std::move(inputs), settings);
    return {std::move(step), QueryPipelineBuilder::getPipeline(std::move(*result))};
}

BarrierTransform::TagsExtractor makeExtractor(
    bool use_same_tags,
    std::shared_ptr<std::atomic_size_t> calls = {},
    std::optional<size_t> throw_on_call = {})
{
    return [use_same_tags, calls_counter = std::move(calls), throw_on_call](const Chunk & chunk)
    {
        size_t call = 0;
        if (calls_counter)
            call = calls_counter->fetch_add(1, std::memory_order_relaxed) + 1;
        if (throw_on_call && call == *throw_on_call)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected PromQL tags extractor failure");

        const auto & ids = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().at(0));
        BarrierTransform::TagsVector tags;
        tags.reserve(ids.size());
        for (size_t i = 0; i != ids.size(); ++i)
            tags.emplace_back(makeTags(use_same_tags ? 0 : ids.getElement(i)));

        return BarrierTransform::NativeTagsChunk{chunk.getColumns().at(0), std::move(tags)};
    };
}

std::vector<UInt64> pullUInt64(QueryPipeline & pipeline)
{
    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> values;
    Chunk chunk;
    while (executor.pull(chunk))
    {
        const auto & column = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().at(0));
        for (size_t i = 0; i != column.size(); ++i)
            values.emplace_back(column.getElement(i));
    }
    return values;
}

}

TEST(PromQLNativeTagsBarrier, DrainsTagsBeforeOpeningMainAndPreservesOutput)
{
    const auto main_header = makeUInt64Header("value");
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto main_state = std::make_shared<SourceState>();
    auto tags_state = std::make_shared<SourceState>();

    Chunks main_chunks;
    main_chunks.emplace_back(makeUInt64Chunk({10, 20}));
    auto main_source = std::make_shared<CountingSource>(
        main_header,
        std::move(main_chunks),
        main_state,
        [collector] { return collector->isNativeSeriesDictionaryBuilt(); });

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1}));
    tags_chunks.emplace_back(makeUInt64Chunk({2}));
    auto tags_source = std::make_shared<CountingSource>(
        tags_header,
        std::move(tags_chunks),
        tags_state,
        [main_state] { return main_state->generate_calls.load(std::memory_order_relaxed) == 0; });

    auto built = buildBarrierPipeline(
        main_header, main_source, tags_header, tags_source, collector, makeExtractor(false));
    const auto values = pullUInt64(built.pipeline);

    EXPECT_EQ(values, (std::vector<UInt64>{10, 20}));
    EXPECT_TRUE(collector->isNativeSeriesDictionaryBuilt());
    EXPECT_FALSE(main_state->precondition_failed.load(std::memory_order_relaxed));
    EXPECT_FALSE(tags_state->precondition_failed.load(std::memory_order_relaxed));
    EXPECT_GT(tags_state->generate_calls.load(std::memory_order_relaxed), 0u);
    EXPECT_GT(main_state->generate_calls.load(std::memory_order_relaxed), 0u);
}

TEST(PromQLNativeTagsBarrier, DuplicateTagsFailBeforeOpeningMain)
{
    const auto main_header = makeUInt64Header("value");
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto main_state = std::make_shared<SourceState>();
    auto tags_state = std::make_shared<SourceState>();

    Chunks main_chunks;
    main_chunks.emplace_back(makeUInt64Chunk({10}));
    auto main_source = std::make_shared<CountingSource>(main_header, std::move(main_chunks), main_state);

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1, 2}));
    auto tags_source = std::make_shared<CountingSource>(tags_header, std::move(tags_chunks), tags_state);

    auto built = buildBarrierPipeline(
        main_header, main_source, tags_header, tags_source, collector, makeExtractor(true));
    expectExceptionCode([&] { static_cast<void>(pullUInt64(built.pipeline)); }, ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);

    EXPECT_EQ(main_state->generate_calls.load(std::memory_order_relaxed), 0u);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, ExtractorFailureAbortsAfterEarlierTagsChunk)
{
    const auto main_header = makeUInt64Header("value");
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto main_state = std::make_shared<SourceState>();
    auto tags_state = std::make_shared<SourceState>();
    auto extractor_calls = std::make_shared<std::atomic_size_t>(0);

    Chunks main_chunks;
    main_chunks.emplace_back(makeUInt64Chunk({10}));
    auto main_source = std::make_shared<CountingSource>(main_header, std::move(main_chunks), main_state);

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1}));
    tags_chunks.emplace_back(makeUInt64Chunk({2}));
    auto tags_source = std::make_shared<CountingSource>(tags_header, std::move(tags_chunks), tags_state);

    auto built = buildBarrierPipeline(
        main_header,
        main_source,
        tags_header,
        tags_source,
        collector,
        makeExtractor(false, extractor_calls, 2));
    expectExceptionCode([&] { static_cast<void>(pullUInt64(built.pipeline)); }, ErrorCodes::BAD_ARGUMENTS);

    EXPECT_EQ(extractor_calls->load(std::memory_order_relaxed), 2u);
    EXPECT_EQ(main_state->generate_calls.load(std::memory_order_relaxed), 0u);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, CancellationWhileTagsAreBlockedDoesNotOpenMainOrSeal)
{
    const auto main_header = makeUInt64Header("value");
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto main_state = std::make_shared<SourceState>();
    auto blocking_state = std::make_shared<BlockingSourceState>();

    Chunks main_chunks;
    main_chunks.emplace_back(makeUInt64Chunk({10}));
    auto main_source = std::make_shared<CountingSource>(main_header, std::move(main_chunks), main_state);
    auto tags_source = std::make_shared<BlockingSource>(tags_header, blocking_state);

    auto built = buildBarrierPipeline(
        main_header, main_source, tags_header, tags_source, collector, makeExtractor(false));
    PullingAsyncPipelineExecutor executor(built.pipeline);
    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk, 1));

    {
        std::unique_lock lock(blocking_state->mutex);
        blocking_state->cv.wait(lock, [&] { return blocking_state->entered; });
    }

    executor.cancel();

    EXPECT_EQ(blocking_state->generate_calls.load(std::memory_order_relaxed), 1u);
    EXPECT_EQ(main_state->generate_calls.load(std::memory_order_relaxed), 0u);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, CancellationBeforeFirstWorkDoesNotStartBuild)
{
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto extractor_calls = std::make_shared<std::atomic_size_t>(0);
    auto barrier = std::make_shared<BarrierTransform>(
        tags_header, collector, makeExtractor(false, extractor_calls));

    barrier->cancel(IProcessor::CancelReason::CancelledByUser);
    expectExceptionCode([&] { barrier->work(); }, ErrorCodes::QUERY_WAS_CANCELLED);

    EXPECT_EQ(extractor_calls->load(std::memory_order_relaxed), 0u);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, CancellationDuringStartDoesNotDeadlockOrSeal)
{
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto tags_state = std::make_shared<SourceState>();
    auto gate = std::make_shared<StartCancelGate>();

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1}));
    auto tags_source = std::make_shared<CountingSource>(
        tags_header, std::move(tags_chunks), tags_state);

    auto barrier = std::make_shared<BarrierTransform>(
        tags_header,
        collector,
        makeExtractor(false),
        BarrierTransform::BeforeSealHook{},
        BarrierTransform::AfterSealHook{},
        [gate]
        {
            std::unique_lock lock(gate->mutex);
            gate->start_entered = true;
            gate->cv.notify_all();
            gate->cv.wait(lock, [&] { return gate->start_released; });
        },
        [gate]
        {
            std::lock_guard lock(gate->mutex);
            gate->cancellation_transitioned = true;
            gate->cv.notify_all();
        });

    Pipe pipe(tags_source);
    pipe.addTransform(barrier);
    QueryPipeline pipeline(std::move(pipe));
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk, 1));

    {
        std::unique_lock lock(gate->mutex);
        gate->cv.wait(lock, [&] { return gate->start_entered; });
    }

    std::jthread cancellation_thread(
        [barrier] { barrier->cancel(IProcessor::CancelReason::CancelledByUser); });

    {
        std::unique_lock lock(gate->mutex);
        gate->cv.wait(lock, [&] { return gate->cancellation_transitioned; });
        gate->start_released = true;
    }
    gate->cv.notify_all();
    cancellation_thread.join();

    expectExceptionCode([&] { executor.cancel(); }, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, CancellationDuringExtractorDoesNotStoreOrSeal)
{
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto tags_state = std::make_shared<SourceState>();
    auto extractor_gate = std::make_shared<ExtractorGate>();

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1}));
    auto tags_source = std::make_shared<CountingSource>(
        tags_header, std::move(tags_chunks), tags_state);

    auto extractor = [extractor_gate](const Chunk & chunk)
    {
        {
            std::unique_lock lock(extractor_gate->mutex);
            extractor_gate->entered = true;
            extractor_gate->cv.notify_all();
            extractor_gate->cv.wait(lock, [&] { return extractor_gate->released; });
        }

        const auto & ids = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().at(0));
        BarrierTransform::TagsVector tags;
        tags.reserve(ids.size());
        for (size_t i = 0; i != ids.size(); ++i)
            tags.emplace_back(makeTags(ids.getElement(i)));
        return BarrierTransform::NativeTagsChunk{chunk.getColumns().at(0), std::move(tags)};
    };

    auto barrier = std::make_shared<BarrierTransform>(tags_header, collector, std::move(extractor));
    Pipe pipe(tags_source);
    pipe.addTransform(barrier);
    QueryPipeline pipeline(std::move(pipe));
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk, 1));

    {
        std::unique_lock lock(extractor_gate->mutex);
        extractor_gate->cv.wait(lock, [&] { return extractor_gate->entered; });
    }

    barrier->cancel(IProcessor::CancelReason::CancelledByUser);
    {
        std::lock_guard lock(extractor_gate->mutex);
        extractor_gate->released = true;
    }
    extractor_gate->cv.notify_all();

    expectExceptionCode([&] { executor.cancel(); }, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, CancellationAfterTagsEOFBeforeSealDoesNotPublishDictionary)
{
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto tags_state = std::make_shared<SourceState>();
    auto seal_gate = std::make_shared<SealGate>();

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1}));
    auto tags_source = std::make_shared<CountingSource>(
        tags_header, std::move(tags_chunks), tags_state);

    auto barrier = std::make_shared<BarrierTransform>(
        tags_header,
        collector,
        makeExtractor(false),
        [seal_gate]
        {
            std::unique_lock lock(seal_gate->mutex);
            seal_gate->entered = true;
            seal_gate->cv.notify_all();
            seal_gate->cv.wait(lock, [&] { return seal_gate->released; });
        });

    Pipe pipe(tags_source);
    pipe.addTransform(barrier);
    QueryPipeline pipeline(std::move(pipe));
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk, 1));

    {
        std::unique_lock lock(seal_gate->mutex);
        seal_gate->cv.wait(lock, [&] { return seal_gate->entered; });
    }

    barrier->cancel(IProcessor::CancelReason::CancelledByUser);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());

    {
        std::lock_guard lock(seal_gate->mutex);
        seal_gate->released = true;
    }
    seal_gate->cv.notify_all();

    expectExceptionCode([&] { executor.cancel(); }, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

TEST(PromQLNativeTagsBarrier, CancellationAfterSealBeforePublicationDoesNotReturnSuccess)
{
    const auto tags_header = makeUInt64Header("id");
    auto collector = std::make_shared<Collector>();
    auto tags_state = std::make_shared<SourceState>();
    auto seal_gate = std::make_shared<SealGate>();

    Chunks tags_chunks;
    tags_chunks.emplace_back(makeUInt64Chunk({1}));
    auto tags_source = std::make_shared<CountingSource>(
        tags_header, std::move(tags_chunks), tags_state);

    auto barrier = std::make_shared<BarrierTransform>(
        tags_header,
        collector,
        makeExtractor(false),
        BarrierTransform::BeforeSealHook{},
        [seal_gate]
        {
            std::unique_lock lock(seal_gate->mutex);
            seal_gate->entered = true;
            seal_gate->cv.notify_all();
            seal_gate->cv.wait(lock, [&] { return seal_gate->released; });
        });

    Pipe pipe(tags_source);
    pipe.addTransform(barrier);
    QueryPipeline pipeline(std::move(pipe));
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk, 1));

    {
        std::unique_lock lock(seal_gate->mutex);
        seal_gate->cv.wait(lock, [&] { return seal_gate->entered; });
    }

    /// The hook is immediately before the Active -> Published CAS. cancel()
    /// must therefore win Active -> Cancelled without waiting for generate().
    barrier->cancel(IProcessor::CancelReason::CancelledByUser);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());

    {
        std::lock_guard lock(seal_gate->mutex);
        seal_gate->released = true;
    }
    seal_gate->cv.notify_all();

    expectExceptionCode([&] { executor.cancel(); }, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_FALSE(collector->isNativeSeriesDictionaryBuilt());
}

}
