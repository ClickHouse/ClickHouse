#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <utility>

#include <gtest/gtest.h>

#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>

#include <unistd.h>

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

SharedHeader dataHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

String makePartialBlob(UInt64 from, UInt64 to, const RuntimeFilterGeometry & geometry = testGeometry())
{
    ApproximateRuntimeFilter filter(0, std::make_shared<DataTypeUInt64>(), geometry, /*distinct_keys_hint_=*/std::nullopt);
    auto column = ColumnUInt64::create();
    for (UInt64 i = from; i < to; ++i)
        column->insertValue(i);
    filter.insert(std::move(column));

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

Chunk makeDataChunk(size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        column->insertValue(i);
    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), rows);
}

struct Gate
{
    std::mutex mutex;
    std::condition_variable cv;
    bool open = false;

    void openGate()
    {
        {
            std::lock_guard lock(mutex);
            open = true;
        }
        cv.notify_all();
    }

    /// Returns false on timeout, so a wiring bug fails the test instead of hanging it.
    bool wait()
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(30), [this] { return open; });
    }
};

/// Delivers its chunk only after the gate is opened.
class GatedSource final : public ISource
{
public:
    GatedSource(SharedHeader header, std::shared_ptr<Gate> gate_, Chunk chunk_)
        : ISource(std::move(header))
        , gate(std::move(gate_))
        , chunk(std::move(chunk_))
    {
    }

    String getName() const override { return "GatedSource"; }

protected:
    Chunk generate() override
    {
        if (!gate->wait())
            return {};
        return std::move(chunk);
    }

private:
    std::shared_ptr<Gate> gate;
    Chunk chunk;
};

class CountingSink final : public ISink
{
public:
    CountingSink(SharedHeader header, std::atomic<size_t> & rows_, size_t open_gate_at_ = 0, std::shared_ptr<Gate> gate_ = nullptr)
        : ISink(std::move(header))
        , rows(rows_)
        , open_gate_at(open_gate_at_)
        , gate(std::move(gate_))
    {
    }

    String getName() const override { return "CountingSink"; }

protected:
    void consume(Chunk chunk) override
    {
        rows += chunk.getNumRows();
        if (gate && rows >= open_gate_at)
            gate->openGate();
    }

private:
    std::atomic<size_t> & rows;
    const size_t open_gate_at;
    std::shared_ptr<Gate> gate;
};

/// Never requests input, like a remote exchange sink whose consumer has not connected yet:
/// scheduling never reaches anything upstream of it.
class NeverReadySink final : public IProcessor
{
public:
    explicit NeverReadySink(SharedHeader header)
        : IProcessor({std::move(header)}, {})
    {
        [[maybe_unused]] int rc = pipe(fds);
        chassert(rc == 0);
    }

    ~NeverReadySink() override
    {
        close(fds[0]);
        close(fds[1]);
    }

    String getName() const override { return "NeverReadySink"; }

    Status prepare() override
    {
        if (isCancelled())
        {
            inputs.front().close();
            return Status::Finished;
        }
        return Status::Async;
    }

    int schedule() override { return fds[0]; }

private:
    int fds[2] = {-1, -1};
};

struct TestPipeline
{
    std::shared_ptr<Processors> processors = std::make_shared<Processors>();
    RuntimeFilterLookupPtr lookup = createRuntimeFilterLookup();
    std::atomic<size_t> data_rows = 0;
    RuntimeFilterGeometry geometry = testGeometry();
    UInt64 max_received_state_bytes = MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES;

    /// Data ports pass through; partial sources feed the merge, which ends in its own sink so the
    /// executor schedules the filter branch independently of data-side demand.
    void build(
        Processors filter_inputs,
        ProcessorPtr data_source,
        size_t open_gate_at = 0,
        std::shared_ptr<Gate> gate = nullptr,
        ProcessorPtr data_sink = nullptr)
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

        OutputPortRawPtrs data_ports{&data_source->getOutputs().front()};
        Processors cluster;
        for (auto * port : data_ports)
        {
            auto pass = std::make_shared<ResizeProcessor>(port->getSharedHeader(), 1, 1);
            connect(*port, pass->getInputs().front());
            cluster.emplace_back(std::move(pass));
        }

        auto input = transform->getInputs().begin();
        for (auto & source : filter_inputs)
        {
            connect(source->getOutputs().front(), *input++);
            cluster.emplace_back(std::move(source));
        }

        auto sink = std::make_shared<EmptySink>(transform->getOutputs().front().getSharedHeader());
        connect(transform->getOutputs().front(), sink->getPort());
        cluster.emplace_back(std::move(transform));
        cluster.emplace_back(std::move(sink));

        OutputPort * data_out = nullptr;
        for (const auto & processor : cluster)
            for (auto & out : processor->getOutputs())
                if (!out.isConnected())
                    data_out = &out;
        ASSERT_NE(data_out, nullptr);

        if (!data_sink)
            data_sink = std::make_shared<CountingSink>(dataHeader(), data_rows, open_gate_at, std::move(gate));
        connect(*data_out, data_sink->getInputs().front());

        processors->emplace_back(std::move(data_source));
        for (auto & processor : cluster)
            processors->emplace_back(std::move(processor));
        processors->emplace_back(std::move(data_sink));
    }

    void execute(size_t num_threads = 4)
    {
        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        executor.execute(num_threads, false);
    }

    /// Executes while another thread waits for the filter registration (30 s failsafe), then
    /// cancels. Returns whether the registration was observed.
    bool executeUntilRegistered()
    {
        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        std::atomic<bool> registered = false;
        std::thread watcher(
            [&]
            {
                for (int i = 0; i < 300 && !registered; ++i)
                {
                    if (lookup->find("test_key"))
                        registered = true;
                    else
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                executor.cancel();
            });
        try
        {
            executor.execute(4, false);
        }
        catch (...) /// NOLINT(bugprone-empty-catch): a cancelled execution may or may not throw
        {
        }
        watcher.join();
        return registered;
    }

    /// Executes while another thread cancels as soon as `cancel_after` opens, then releases
    /// `release_after_cancel` so a source blocked on it can finish.
    void executeAndCancel(const std::shared_ptr<Gate> & cancel_after, const std::shared_ptr<Gate> & release_after_cancel)
    {
        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        std::thread canceller(
            [&]
            {
                cancel_after->wait();
                executor.cancel();
                release_after_cancel->openGate();
            });
        try
        {
            executor.execute(4, false);
        }
        catch (...) /// NOLINT(bugprone-empty-catch): a cancelled execution may or may not throw
        {
        }
        canceller.join();
    }
};

std::vector<bool> probe(const IRuntimeFilter & filter, UInt64 from, UInt64 to)
{
    auto column = ColumnUInt64::create();
    for (UInt64 i = from; i < to; ++i)
        column->insertValue(i);
    auto result = filter.find({std::move(column), std::make_shared<DataTypeUInt64>(), "probe"});
    auto full = result->convertToFullColumnIfConst();
    std::vector<bool> found(to - from);
    for (size_t i = 0; i < found.size(); ++i)
        found[i] = full->getUInt(i) != 0;
    return found;
}

ProcessorPtr partialSource(UInt64 from, UInt64 to)
{
    return std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({makePartialBlob(from, to)}));
}

}

TEST(MergeRuntimeFiltersTransform, AllPartialsRegisterUnion)
{
    TestPipeline pipeline;
    pipeline.build(
        {partialSource(10, 20), partialSource(20, 30), partialSource(0, 10)},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)));
    pipeline.execute();

    EXPECT_EQ(pipeline.data_rows, 1000u);

    auto filter = pipeline.lookup->find("test_key");
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(probe(*filter, 0, 30), std::vector<bool>(30, true));
    EXPECT_EQ(probe(*filter, 30, 40), std::vector<bool>(10, false));
}

TEST(MergeRuntimeFiltersTransform, MissingPartialFailsOpen)
{
    TestPipeline pipeline;
    pipeline.build(
        {partialSource(10, 20), std::make_shared<NullSource>(runtimeFilterPartialsHeader()), partialSource(0, 10)},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)));
    pipeline.execute();

    EXPECT_EQ(pipeline.data_rows, 1000u);
    EXPECT_EQ(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, NoPartialsAtAllFailsOpen)
{
    TestPipeline pipeline;
    pipeline.build(
        {std::make_shared<NullSource>(runtimeFilterPartialsHeader()), std::make_shared<NullSource>(runtimeFilterPartialsHeader())},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)));
    pipeline.execute();

    EXPECT_EQ(pipeline.data_rows, 1000u);
    EXPECT_EQ(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, DataFlowsBeforeFiltersArrive)
{
    /// The partials are released only after every data row has been consumed: if the transform held
    /// the data stream back, this would deadlock (the gate would never open) and time out.
    auto gate = std::make_shared<Gate>();
    TestPipeline pipeline;
    pipeline.build(
        {std::make_shared<GatedSource>(runtimeFilterPartialsHeader(), gate, makePartialsChunk({makePartialBlob(0, 10)}))},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)),
        /*open_gate_at=*/1000,
        gate);
    pipeline.execute();

    EXPECT_EQ(pipeline.data_rows, 1000u);
    ASSERT_NE(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, RegistersWithoutDataSinkDemand)
{
    /// On a remote worker the data sink may take nothing until the join pulls the probe side,
    /// which transitively waits for this very registration: the filter branch must run without
    /// any demand from the data side.
    TestPipeline pipeline;
    pipeline.build(
        {partialSource(0, 10), partialSource(10, 20)},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)),
        /*open_gate_at=*/0,
        /*gate=*/nullptr,
        std::make_shared<NeverReadySink>(dataHeader()));
    EXPECT_TRUE(pipeline.executeUntilRegistered());

    auto filter = pipeline.lookup->find("test_key");
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(probe(*filter, 0, 20), std::vector<bool>(20, true));
}

TEST(MergeRuntimeFiltersTransform, CancelWhileWaitingForFilters)
{
    /// Cancellation while the filter stream is still pending must terminate the pipeline.
    auto drained = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    TestPipeline pipeline;
    pipeline.build(
        {std::make_shared<GatedSource>(runtimeFilterPartialsHeader(), release, makePartialsChunk({makePartialBlob(0, 10)}))},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)),
        /*open_gate_at=*/1000,
        drained);
    pipeline.executeAndCancel(drained, release);

    EXPECT_EQ(pipeline.data_rows, 1000u);
}

TEST(MergeRuntimeFiltersTransform, DuplicatePartialFromOneSourceThrows)
{
    const String blob = makePartialBlob(0, 10);
    TestPipeline pipeline;
    pipeline.build(
        {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob, blob}))},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(10)));
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

TEST(MergeRuntimeFiltersTransform, MalformedPartialThrows)
{
    TestPipeline pipeline;
    pipeline.build(
        {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({"garbage"}))},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(10)));
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
    /// A state bigger than the limit must not fail the query: it is rejected, nothing is
    /// registered, and data rows keep flowing unfiltered.
    const String blob = makePartialBlob(0, 10);
    TestPipeline pipeline;
    pipeline.max_received_state_bytes = blob.size() - 1;
    pipeline.build(
        {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob})), partialSource(10, 20)},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(1000)));
    pipeline.execute();

    EXPECT_EQ(pipeline.data_rows, 1000u);
    EXPECT_EQ(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, StateExactlyAtLimitIsAccepted)
{
    const String blob = makePartialBlob(0, 10);
    TestPipeline pipeline;
    pipeline.max_received_state_bytes = blob.size();
    pipeline.build(
        {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob}))},
        std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(10)));
    pipeline.execute();

    ASSERT_NE(pipeline.lookup->find("test_key"), nullptr);
}

TEST(MergeRuntimeFiltersTransform, LimitNearMaxDoesNotOverflow)
{
    TestPipeline pipeline;
    pipeline.max_received_state_bytes = std::numeric_limits<UInt64>::max();
    pipeline.build({partialSource(0, 10)}, std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(10)));
    pipeline.execute();

    ASSERT_NE(pipeline.lookup->find("test_key"), nullptr);
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
std::vector<String> runForward(
    Processors sources,
    UInt64 max_received_state_bytes = MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES,
    const RuntimeFilterGeometry & geometry = testGeometry())
{
    auto processors = std::make_shared<Processors>();
    auto transform = std::make_shared<MergeRuntimeFiltersTransform>(
        runtimeFilterPartialsHeader(),
        sources.size(),
        MergeRuntimeFiltersTransform::Mode::ForwardUnion,
        "test_filter",
        /*filter_key_=*/"",
        std::make_shared<DataTypeUInt64>(),
        geometry,
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
    PipelineExecutor executor(processors, element);
    executor.execute(1, false);
    return blobs;
}

std::unique_ptr<ApproximateRuntimeFilter> deserializeBlob(const String & blob)
{
    ReadBufferFromString in(blob);
    return ApproximateRuntimeFilter::deserialize(in, 0, std::make_shared<DataTypeUInt64>(), testGeometry());
}

}

TEST(MergeRuntimeFiltersTransform, ForwardModeEmitsUnion)
{
    auto blobs = runForward({partialSource(0, 10), partialSource(10, 20), partialSource(20, 30)});
    ASSERT_EQ(blobs.size(), 1u);

    auto merged = deserializeBlob(blobs.front());
    merged->finishInsert();
    EXPECT_EQ(probe(*merged, 0, 30), std::vector<bool>(30, true));
    EXPECT_EQ(probe(*merged, 30, 40), std::vector<bool>(10, false));
}

TEST(MergeRuntimeFiltersTransform, ForwardModeMissingInputEmitsNothing)
{
    auto blobs = runForward({partialSource(0, 10), std::make_shared<NullSource>(runtimeFilterPartialsHeader())});
    EXPECT_TRUE(blobs.empty());
}

TEST(MergeRuntimeFiltersTransform, ForwardModeOversizedInputEmitsNothing)
{
    const String blob = makePartialBlob(0, 10);
    auto blobs = runForward(
        {std::make_shared<SourceFromSingleChunk>(runtimeFilterPartialsHeader(), makePartialsChunk({blob})), partialSource(10, 20)},
        /*max_received_state_bytes=*/blob.size() - 1);
    EXPECT_TRUE(blobs.empty());
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

TEST(MergeRuntimeFiltersTransform, ForwardModeSingleInputPassesThrough)
{
    auto blobs = runForward({partialSource(0, 10)});
    ASSERT_EQ(blobs.size(), 1u);

    auto merged = deserializeBlob(blobs.front());
    merged->finishInsert();
    EXPECT_EQ(probe(*merged, 0, 10), std::vector<bool>(10, true));
    EXPECT_EQ(probe(*merged, 10, 20), std::vector<bool>(10, false));
}

TEST(MergeRuntimeFiltersTransform, PayloadRetentionIndependentOfInputCount)
{
    /// The transform must merge each arriving state immediately instead of retaining the
    /// serialized payloads: with equally sized bloom states, the peak allocation of the merge
    /// must not grow with the input count. Retaining (or copying) the payloads would add
    /// (num_inputs x payload) to the peak; the margin below is a few payloads.
    ///
    /// The measurement reads the current thread's memory tracker. Take the `current_thread` slot
    /// for this test only; the process-lifetime `MainThreadStatus` would leave it set forever, and
    /// any later fixture constructing its own `ThreadStatus` would assert on the occupied slot.
    ThreadStatus * previous_thread_status = std::exchange(current_thread, nullptr);
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
                    makePartialsChunk({makePartialBlob(input * 1000, input * 1000 + 100, geometry)})));
        pipeline.build(std::move(sources), std::make_shared<SourceFromSingleChunk>(dataHeader(), makeDataChunk(10)));

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
}
