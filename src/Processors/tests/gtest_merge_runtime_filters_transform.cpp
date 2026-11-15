#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <utility>

#include <gtest/gtest.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{

constexpr UInt64 BLOOM_BYTES = 4096;
constexpr UInt64 EXACT_VALUES_LIMIT = 64;
constexpr UInt64 HASH_FUNCTIONS = 3;

RuntimeFilterGeometry testGeometry()
{
    return RuntimeFilterGeometry{
        .exact_values_limit = EXACT_VALUES_LIMIT,
        .exact_bytes_limit = BLOOM_BYTES,
        .bloom_filter_bytes = BLOOM_BYTES,
        .bloom_filter_hash_functions = HASH_FUNCTIONS,
        .pass_ratio_threshold_for_disabling = 1.0,
        .blocks_to_skip_before_reenabling = 0,
        .max_ratio_of_set_bits_in_bloom_filter = 1.0,
    };
}

AdaptiveSetRuntimeFilter
makePartial(UInt64 from, UInt64 to, bool track_key_range = false, const RuntimeFilterGeometry & geometry = testGeometry())
{
    AdaptiveSetRuntimeFilter filter(
        std::make_shared<DataTypeUInt64>(), geometry,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    filter.boundExactSetByKeyBytes();
    if (track_key_range)
        filter.enableKeyRangeTracking();
    auto column = ColumnUInt64::create();
    for (UInt64 i = from; i < to; ++i)
        column->insertValue(i);
    filter.insert(std::move(column));
    return filter;
}

String makePartialBlob(UInt64 from, UInt64 to, bool track_key_range = false, const RuntimeFilterGeometry & geometry = testGeometry())
{
    auto filter = makePartial(from, to, track_key_range, geometry);

    WriteBufferFromOwnString out;
    filter.serialize(out);
    return out.str();
}

Chunk makePartialsChunk(const std::vector<String> & blobs)
{
    auto column = ColumnString::create();
    for (const auto & blob : blobs)
        column->insertData(blob.data(), blob.size());
    const size_t rows = blobs.size();
    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), rows);
}

struct TestPipeline
{
    std::shared_ptr<Processors> processors = std::make_shared<Processors>();
    RuntimeFilterLookupPtr lookup = createRuntimeFilterLookup();
    RuntimeFilterGeometry geometry = testGeometry();
    UInt64 max_received_state_bytes = MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES;
    bool enable_index_analysis = false;

    /// Partial sources -> register-mode merge -> empty sink.
    void build(Processors filter_inputs)
    {
        auto transform = std::make_shared<MergeRuntimeFiltersTransform>(
            runtimeFilterPartialsHeader(),
            filter_inputs.size(),
            MergeRuntimeFiltersTransform::Mode::RegisterUnion,
            "test_filter",
            "test_key",
            std::make_shared<DataTypeUInt64>(),
            geometry,
            lookup,
            /*num_forward_destinations_=*/1,
            max_received_state_bytes);
        if (enable_index_analysis)
            transform->enableIndexAnalysis();

        auto input = transform->getInputs().begin();
        for (auto & source : filter_inputs)
        {
            connect(source->getOutputs().front(), *input++);
            processors->emplace_back(std::move(source));
        }

        auto sink = std::make_shared<EmptySink>(transform->getOutputs().front().getSharedHeader());
        connect(transform->getOutputs().front(), sink->getPort());
        processors->emplace_back(std::move(transform));
        processors->emplace_back(std::move(sink));
    }

    void execute(size_t num_threads = 4) const
    {
        QueryStatusPtr element;
        Runtime::V1::PipelineExecutor executor(processors, element);
        executor.execute(num_threads, false);
    }
};

ColumnWithTypeAndName probeColumn(UInt64 from, UInt64 to)
{
    auto column = ColumnUInt64::create();
    for (UInt64 i = from; i < to; ++i)
        column->insertValue(i);
    return ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "probe");
}

std::vector<bool> maskToVector(const ColumnPtr & mask, size_t rows)
{
    auto full = mask->convertToFullColumnIfConst();
    std::vector<bool> found(rows);
    for (size_t i = 0; i < found.size(); ++i)
        found[i] = full->getUInt(i) != 0;
    return found;
}

std::vector<bool> probe(const RuntimeFilter & filter, UInt64 from, UInt64 to)
{
    return maskToVector(filter.find(probeColumn(from, to)), to - from);
}

std::vector<bool> probe(const AdaptiveSetRuntimeFilter & filter, UInt64 from, UInt64 to)
{
    std::optional<size_t> rows_passed;
    return maskToVector(filter.find(probeColumn(from, to), rows_passed), to - from);
}

/// A forwarded union is not wrapped in a `RuntimeFilter`, so it has no evaluation state; lend it a
/// private one to finish the build, the way the transport transforms do.
void finishInsert(AdaptiveSetRuntimeFilter & filter)
{
    const auto geometry = testGeometry();
    RuntimeFilterEvaluationState evaluation_state(
        RuntimeFilterConfig{geometry.pass_ratio_threshold_for_disabling, geometry.blocks_to_skip_before_reenabling});
    filter.finishInsert(evaluation_state);
}

ProcessorPtr partialSource(UInt64 from, UInt64 to, bool track_key_range = false)
{
    return std::make_shared<SourceFromSingleChunk>(
        runtimeFilterPartialsHeader(), makePartialsChunk({makePartialBlob(from, to, track_key_range)}));
}

ProcessorPtr blobSource(const String & blob)
{
    return std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob}));
}

void expectRange(const std::optional<Range> & range, UInt64 min, UInt64 max)
{
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->left, Field(min));
    EXPECT_EQ(range->right, Field(max));
}

}

TEST(MergeRuntimeFiltersTransform, AllPartialsRegisterUnion)
{
    TestPipeline pipeline;
    pipeline.enable_index_analysis = true;
    pipeline.build({partialSource(10, 20), partialSource(20, 30), partialSource(0, 10)});
    pipeline.execute();

    auto filter = pipeline.lookup->find("test_key");
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(probe(*filter, 0, 30), std::vector<bool>(30, true));
    EXPECT_EQ(probe(*filter, 30, 40), std::vector<bool>(10, false));

    /// The exact union is exposed for index analysis.
    const auto values = filter->getRecordedKeyValues();
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->size(), 30u);
}

TEST(MergeRuntimeFiltersTransform, MissingPartialFailsOpen)
{
    TestPipeline pipeline;
    pipeline.build({partialSource(10, 20), std::make_shared<NullSource>(runtimeFilterPartialsHeader()), partialSource(0, 10)});
    pipeline.execute();

    EXPECT_EQ(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, DuplicatePartialFromOneSourceThrows)
{
    const String blob = makePartialBlob(0, 10);
    TestPipeline pipeline;
    pipeline.build({std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob, blob}))});
    try
    {
        pipeline.execute();
        FAIL() << "expected an exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
    EXPECT_EQ(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, OversizedStateFailsOpen)
{
    /// A state bigger than the limit must not fail the query: it is rejected and nothing is registered.
    const String blob = makePartialBlob(0, 10);
    {
        TestPipeline pipeline;
        pipeline.max_received_state_bytes = blob.size() - 1;
        pipeline.build(
            {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob})), partialSource(10, 20)});
        pipeline.execute();
        EXPECT_EQ(pipeline.lookup->find("test_key"), nullptr);
    }

    /// A state exactly at the limit is accepted.
    {
        TestPipeline pipeline;
        pipeline.max_received_state_bytes = blob.size();
        pipeline.build({std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob}))});
        pipeline.execute();
        EXPECT_NE(pipeline.lookup->find("test_key"), nullptr);
    }
}

namespace
{

/// Collects the serialized states a forward-mode transform emits.
class CollectBlobsSink final : public ISink
{
public:
    CollectBlobsSink(SharedHeader header, std::vector<String> & blobs_)
        : ISink(std::move(header))
        , blobs(blobs_)
    {
    }

    String getName() const override { return "CollectBlobsSink"; }

protected:
    void consume(Chunk chunk) override
    {
        const auto & column = assert_cast<const ColumnString &>(*chunk.getColumns().front());
        for (size_t row = 0; row < column.size(); ++row)
            blobs.emplace_back(column.getDataAt(row));
    }

private:
    std::vector<String> & blobs;
};

/// Runs [sources -> forward-mode transform -> collecting sink] and returns the emitted states.
std::vector<String> runForward(Processors sources, UInt64 max_received_state_bytes = MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES)
{
    auto processors = std::make_shared<Processors>();
    auto transform = std::make_shared<MergeRuntimeFiltersTransform>(
        runtimeFilterPartialsHeader(),
        sources.size(),
        MergeRuntimeFiltersTransform::Mode::ForwardUnion,
        "test_filter",
        /*filter_key_=*/"",
        std::make_shared<DataTypeUInt64>(),
        testGeometry(),
        /*filter_lookup_=*/nullptr,
        /*num_forward_destinations_=*/1,
        max_received_state_bytes);

    auto input = transform->getInputs().begin();
    for (auto & source : sources)
    {
        connect(source->getOutputs().front(), *input++);
        processors->emplace_back(std::move(source));
    }

    std::vector<String> blobs;
    auto sink = std::make_shared<CollectBlobsSink>(runtimeFilterPartialsHeader(), blobs);
    connect(transform->getOutputs().front(), sink->getInputs().front());
    processors->emplace_back(std::move(transform));
    processors->emplace_back(std::move(sink));

    QueryStatusPtr element;
    Runtime::V1::PipelineExecutor executor(processors, element);
    executor.execute(1, false);
    return blobs;
}

AdaptiveSetRuntimeFilter deserializeBlob(const String & blob)
{
    ReadBufferFromString in(blob);
    return AdaptiveSetRuntimeFilter::deserialize(in, std::make_shared<DataTypeUInt64>(), testGeometry());
}

}

TEST(MergeRuntimeFiltersTransform, ForwardModeEmitsUnion)
{
    auto blobs = runForward({partialSource(0, 10), partialSource(10, 20), partialSource(20, 30)});
    ASSERT_EQ(blobs.size(), 1u);

    auto merged = deserializeBlob(blobs.front());
    finishInsert(merged);
    EXPECT_EQ(probe(merged, 0, 30), std::vector<bool>(30, true));
    EXPECT_EQ(probe(merged, 30, 40), std::vector<bool>(10, false));
}

/// More than `EXACT_VALUES_LIMIT` keys turn a partial into a bloom filter, which cannot give back its
/// keys, so index analysis has only the key range that travels with the state.
TEST(MergeRuntimeFiltersTransform, KeyRangeSurvivesTheBloomState)
{
    /// A merge task forwards the union; an empty partial must not stop the range tracking.
    auto blobs = runForward({partialSource(100, 300, true), partialSource(5, 5, true), partialSource(1000, 1100, true)});
    ASSERT_EQ(blobs.size(), 1u);
    expectRange(deserializeBlob(blobs.front()).getKeyRange(), 100, 1099);

    TestPipeline pipeline;
    pipeline.enable_index_analysis = true;
    pipeline.build({blobSource(blobs.front()), partialSource(50, 60, true)});
    pipeline.execute();

    auto filter = pipeline.lookup->find("test_key");
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(filter->getRecordedKeyValues(), nullptr);
    expectRange(filter->getRecordedKeyRanges(), 50, 1099);
}

TEST(MergeRuntimeFiltersTransform, KeyRangeIsDroppedWhenAPartialHasNone)
{
    /// The partial without tracking has keys outside the range of the others.
    auto blobs = runForward({partialSource(100, 300, true), partialSource(0, 10)});
    ASSERT_EQ(blobs.size(), 1u);
    EXPECT_FALSE(deserializeBlob(blobs.front()).getKeyRange().has_value());
}

TEST(MergeRuntimeFiltersTransform, SameTaskPublicationKeepsTheKeyRange)
{
    /// A build task registers its own merged partial for the apply sites of its stage.
    auto lookup = createRuntimeFilterLookup();
    registerCompleteRuntimeFilter(
        *lookup, "test_key", "test_filter", testGeometry(), makePartial(200, 400, true), /*enable_index_analysis=*/true);

    auto filter = lookup->find("test_key");
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(filter->getRecordedKeyValues(), nullptr);
    expectRange(filter->getRecordedKeyRanges(), 200, 399);
}

TEST(MergeRuntimeFiltersTransform, ForwardModeOversizedMergedOutputEmitsNothing)
{
    /// Each input is a small exact state, but their union crosses the exact limits and degrades
    /// to a bloom filter bigger than the cap: the merged output must not be emitted.
    const String first = makePartialBlob(0, 50);
    const String second = makePartialBlob(50, 100);
    const UInt64 cap = std::max(first.size(), second.size());
    ASSERT_LT(cap, BLOOM_BYTES);

    auto blobs = runForward(
        {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({first})),
         std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({second}))},
        cap);
    EXPECT_TRUE(blobs.empty());
}

TEST(MergeRuntimeFiltersTransform, PayloadRetentionIndependentOfInputCount)
{
#if defined(ADDRESS_SANITIZER) || defined(MEMORY_SANITIZER) || defined(THREAD_SANITIZER)
    /// `BloomFilter` holds its bits in a `std::vector`, so a payload reaches `MemoryTracker` only
    /// through the tracking `operator new` in `AllocationInterceptors.cpp`. ASan, MSan and TSan
    /// force-load their own `operator new` ahead of `libclickhouse_new_delete.a`, so the peak reads 0.
    /// `00877_memory_limit_for_new_delete.sql` is tagged `no-asan`, `no-msan` and `no-tsan` for the
    /// same reason. UBSan keeps the tracking `operator new`, so the test still runs there.
    GTEST_SKIP() << "Payload accounting needs the tracking `operator new`, which ASan, MSan and TSan replace";
#else
    /// The transform must merge each arriving state immediately instead of retaining the
    /// serialized payloads: with equally sized bloom states, the peak allocation of the merge
    /// must not grow with the input count. Retaining (or copying) the payloads would add
    /// (num_inputs x payload) to the peak; the margin below is a few payloads.
    ///
    /// The measurement reads the current thread's memory tracker. Take the `current_thread` slot
    /// for this test only; the process-lifetime `MainThreadStatus` would leave it set forever, and
    /// any later fixture constructing its own `ThreadStatus` would assert on the occupied slot.
    /// Read the slot before clearing it: `current_thread` is a stateless accessor, so
    /// `std::exchange` would return the value it had just stored, not the previous one.
    ThreadStatus * previous_thread_status = current_thread;
    current_thread = nullptr;
    SCOPE_EXIT({ current_thread = previous_thread_status; });
    ThreadStatus scoped_thread_status;

    RuntimeFilterGeometry geometry = testGeometry();
    geometry.bloom_filter_bytes = 2 * 1024 * 1024;
    geometry.exact_bytes_limit = 2 * 1024 * 1024;
    const UInt64 payload = geometry.bloom_filter_bytes;

    auto peak_for_inputs = [&](size_t num_inputs) -> Int64
    {
        /// Everything (input blobs included) is allocated before the measurement starts; only the
        /// execution itself runs under the scoped tracker.
        TestPipeline pipeline;
        pipeline.geometry = geometry;
        Processors sources;
        for (size_t input = 0; input < num_inputs; ++input)
            sources.push_back(
                std::make_shared<SourceFromSingleChunk>(
                    runtimeFilterPartialsHeader(),
                    /// More rows than `exact_values_limit`, so the state is a bloom of `payload` bytes.
                    makePartialsChunk({makePartialBlob(input * 1000, input * 1000 + 100, /*track_key_range=*/false, geometry)})));
        pipeline.build(std::move(sources));

        auto & thread_tracker = CurrentThread::get().memory_tracker;
        MemoryTracker scoped_tracker(&total_memory_tracker, VariableContext::Process, /*log_peak_memory_usage_in_destructor=*/false);
        MemoryTracker * prev_parent = thread_tracker.getParent();
        const Int64 prev_untracked_limit = CurrentThread::get().untracked_memory_limit;
        CurrentThread::flushUntrackedMemory();
        thread_tracker.setParent(&scoped_tracker);
        CurrentThread::get().untracked_memory_limit = 4 * 1024;
        SCOPE_EXIT_SAFE({
            CurrentThread::flushUntrackedMemory();
            CurrentThread::get().untracked_memory_limit = prev_untracked_limit;
            thread_tracker.setParent(prev_parent);
        });

        pipeline.execute(/*num_threads=*/1);
        CurrentThread::flushUntrackedMemory();

        EXPECT_NE(pipeline.lookup->find("test_key"), nullptr);
        return scoped_tracker.getPeak();
    };

    const Int64 peak_2 = peak_for_inputs(2);
    const Int64 peak_32 = peak_for_inputs(32);

    /// Sanity: the measurement sees at least the registered 2 MiB bloom state.
    EXPECT_GE(peak_2, static_cast<Int64>(payload));
    /// 30 extra inputs of 2 MiB each would add ~60 MiB if retained; allow a few payloads of noise.
    EXPECT_LE(peak_32, peak_2 + static_cast<Int64>(3 * payload));
#endif
}
