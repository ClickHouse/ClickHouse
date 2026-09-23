#include <gtest/gtest.h>

#include <algorithm>
#include <bit>
#include <functional>
#include <initializer_list>
#include <thread>
#include <tuple>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/ExternalDistinctTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/scope_guard.h>

using namespace DB;

namespace
{

Chunk makeChunk(std::initializer_list<UInt64> values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insertValue(value);
    return Chunk(Columns{std::move(column)}, values.size());
}

class MergeCountingChunkInfo : public ChunkInfoCloneable<MergeCountingChunkInfo>
{
public:
    explicit MergeCountingChunkInfo(size_t & merge_calls_)
        : merge_calls(merge_calls_)
    {
    }

    Ptr merge(const Ptr & right) const override
    {
        ++merge_calls;
        return right;
    }

private:
    size_t & merge_calls;
};

struct ConnectedDistinct
{
    SharedHeader header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});
    OutputPort upstream{header};
    InputPort downstream{header};
    ExternalDistinctTransform transform;
    Processors processors;

    static constexpr UInt64 default_spill_threshold = 64 << 20;

    explicit ConnectedDistinct(TemporaryDataOnDiskScopePtr tmp_data, UInt64 limit_hint = 0, UInt64 threshold = default_spill_threshold)
        : transform(header, SizeLimits{}, limit_hint, Names{}, threshold,
            std::move(tmp_data), /*min_free_disk_space_=*/ 0, /*max_block_size_rows_=*/ 2,
            /*preferred_block_bytes_=*/ DEFAULT_BLOCK_SIZE * 256, /*preserve_input_order_=*/ false)
    {
        connect(upstream, transform.getInputs().front());
        connect(transform.getOutputs().front(), downstream);
        downstream.setNeeded();
    }

    /// Populate the set and consume its output before introducing memory pressure for the next chunk.
    void hashChunk(std::initializer_list<UInt64> values)
    {
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        upstream.push(makeChunk(values));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        ASSERT_TRUE(downstream.hasData());
        EXPECT_EQ(downstream.pull().getNumRows(), values.size());
    }

    struct RunProcessors
    {
        BufferingToFileSink * sink = nullptr;
        BufferingFromFileSource * source = nullptr;
    };

    RunProcessors attachRun()
    {
        auto update = transform.updatePipeline();
        RunProcessors run;
        for (const auto & processor : update.to_add)
        {
            if (auto * sink = typeid_cast<BufferingToFileSink *>(processor.get()))
                run.sink = sink;
            else if (auto * source = typeid_cast<BufferingFromFileSource *>(processor.get()))
                run.source = source;
        }
        processors.splice(processors.end(), update.to_add);
        return run;
    }
};

void readFirstSpillRun(ExternalDistinctTransform & transform, Blocks & blocks)
{
    ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
    auto update = transform.updatePipeline();
    auto sink_it = std::ranges::find_if(update.to_add, [](const auto & processor)
    {
        return typeid_cast<BufferingToFileSink *>(processor.get());
    });
    ASSERT_NE(sink_it, update.to_add.end());
    auto & sink = assert_cast<BufferingToFileSink &>(**sink_it);
    while (true)
    {
        const auto sink_status = sink.prepare();
        if (sink_status == IProcessor::Status::Finished)
            break;
        if (sink_status == IProcessor::Status::Ready)
            sink.work();
        else
            ASSERT_EQ(sink_status, IProcessor::Status::NeedData);
        const auto status = transform.prepare();
        if (status == IProcessor::Status::Ready)
            transform.work();
        else
            ASSERT_TRUE(status == IProcessor::Status::NeedData || status == IProcessor::Status::PortFull);
    }
    auto reader = sink.getHolder().getReadStream();
    for (auto block = reader->read(); !block.empty(); block = reader->read())
        blocks.push_back(std::move(block));
}

class ExternalDistinctTransformTest : public testing::Test
{
protected:
    void SetUp() override
    {
        disk = createDisk("external_distinct");
        tmp_data = std::make_shared<TemporaryDataOnDiskScope>(
            TemporaryDataOnDiskSettings{}, std::make_shared<SingleDiskVolume>("temporary", disk));
    }

    void TearDown() override
    {
        tmp_data.reset();
        destroyDisk(disk);
    }

    /// Track query allocations independently of the thread running the test suite.
    void withQueryThread(const std::function<void()> & body)
    {
        MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
        std::thread([&]
        {
            ThreadStatus thread_status;
            thread_status.memory_tracker.setParent(&query);
            thread_status.untracked_memory_limit = 0;
            body();
        }).join();
    }

    DiskPtr disk;
    TemporaryDataOnDiskScopePtr tmp_data;
};

}

TEST_F(ExternalDistinctTransformTest, SpillCompletionIsIndependentOfResultBackpressure)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data);
        auto & transform = connected.transform;
        auto & downstream = connected.downstream;
        ASSERT_NO_FATAL_FAILURE(connected.hashChunk({1, 2, 3, 4, 5, 6}));
        connected.upstream.push(makeChunk({5, 7, 8, 9}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        downstream.setNotNeeded();
        /// Another operator consumes half the threshold. Its remaining budget cannot fit hashing
        /// and spill workspace, although current query memory is still below the threshold.
        std::ignore = CurrentMemoryTracker::alloc(ConnectedDistinct::default_spill_threshold / 2);
        SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(ConnectedDistinct::default_spill_threshold / 2));
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        auto suppression = connected.attachRun();
        ASSERT_TRUE(suppression.sink);
        ASSERT_TRUE(suppression.source);
        auto & suppression_sink = *suppression.sink;
        auto & suppression_source = *suppression.source;
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Ready);
        suppression_sink.work();
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::NeedData);

        /// The three two-row suppression chunks drain while result output is blocked.
        for (size_t i = 0; i < 3; ++i)
        {
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();
            ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Ready);
            suppression_sink.work();
            ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::NeedData);
            EXPECT_FALSE(downstream.hasData());
        }

        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Ready);
        EXPECT_EQ(suppression_source.prepare(), IProcessor::Status::NeedData);
        EXPECT_EQ(transform.prepare(), IProcessor::Status::NeedData);

        /// Finalizing the file releases the suppression dependency, even with output still blocked.
        suppression_sink.work();
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Finished);
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        EXPECT_TRUE(suppression_source.getCompletionPort().isFinished());
        transform.work();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::PortFull);

        downstream.setNeeded();
        /// The chunk rejected before insertion remains available after suppression extraction.
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        EXPECT_TRUE(connected.upstream.isNeeded());
    });
}

TEST_F(ExternalDistinctTransformTest, OrdinaryInputResumesBeforeFileCompletion)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data, /*limit_hint=*/ 0, /*threshold=*/ 1);
        auto & transform = connected.transform;
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        connected.upstream.push(makeChunk({5, 7, 8, 9}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        /// An empty set needs no suppression run. The rejected chunk starts the first ordinary run.
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        auto input = connected.attachRun();
        ASSERT_TRUE(input.sink);
        ASSERT_TRUE(input.source);
        auto & input_sink = *input.sink;
        auto & input_source = *input.source;
        ASSERT_EQ(input_sink.prepare(), IProcessor::Status::Ready);
        input_sink.work();
        ASSERT_EQ(input_sink.prepare(), IProcessor::Status::NeedData);
        for (size_t i = 0; i < 2; ++i)
        {
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();
            ASSERT_EQ(input_sink.prepare(), IProcessor::Status::Ready);
            input_sink.work();
            ASSERT_EQ(input_sink.prepare(), IProcessor::Status::NeedData);
        }

        /// Ordinary input resumes before its file finishes; only the reader waits for that sink.
        EXPECT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        EXPECT_TRUE(connected.upstream.isNeeded());
        EXPECT_FALSE(input_source.getCompletionPort().isFinished());
        EXPECT_EQ(input_source.prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(input_sink.prepare(), IProcessor::Status::Ready);
        input_sink.work();
        EXPECT_EQ(input_sink.prepare(), IProcessor::Status::Finished);
        EXPECT_TRUE(input_source.getCompletionPort().isFinished());
    });
}

TEST_F(ExternalDistinctTransformTest, LimitRetainsPendingResultUntilOutputIsReady)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data, /*limit_hint=*/ 1);
        auto & transform = connected.transform;
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        connected.upstream.push(makeChunk({1, 2}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        connected.downstream.setNotNeeded();
        transform.work();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::PortFull);

        connected.downstream.setNeeded();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::Finished);
        EXPECT_TRUE(connected.upstream.isFinished());
        ASSERT_TRUE(connected.downstream.hasData());
        EXPECT_EQ(connected.downstream.pull().getNumRows(), 2);
        EXPECT_TRUE(connected.downstream.isFinished());
    });
}

TEST_F(ExternalDistinctTransformTest, DownstreamClosureTerminatesRunDependencies)
{
    withQueryThread([&]
    {
        for (bool connect_run : {false, true})
        {
            ConnectedDistinct connected(tmp_data);
            auto & transform = connected.transform;
            ASSERT_NO_FATAL_FAILURE(connected.hashChunk({1, 2, 3}));
            connected.upstream.push(makeChunk({1}));
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            std::ignore = CurrentMemoryTracker::alloc(ConnectedDistinct::default_spill_threshold);
            SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(ConnectedDistinct::default_spill_threshold));
            transform.work();
            ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
            if (connect_run)
                connected.attachRun();

            connected.downstream.close();
            EXPECT_EQ(transform.prepare(), IProcessor::Status::Finished);
            for (const auto & output : transform.getOutputs())
                EXPECT_TRUE(output.isFinished());
            for (const auto & input : transform.getInputs())
                EXPECT_TRUE(input.isFinished());
            if (connect_run)
            {
                auto merger = std::ranges::find_if(connected.processors, [](const auto & processor)
                {
                    return typeid_cast<DistinctSortedTransform *>(processor.get());
                });
                ASSERT_NE(merger, connected.processors.end());
                EXPECT_EQ((*merger)->prepare(), IProcessor::Status::Finished);
            }
        }
    });
}

#ifndef DEBUG_OR_SANITIZER_BUILD

/// `LOGICAL_ERROR` aborts during construction in Debug and sanitizer builds. Release enforces the
/// completion protocol with an exception that the caller can observe.
TEST_F(ExternalDistinctTransformTest, CompletionPortRejectsData)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data);
        auto & transform = connected.transform;
        ASSERT_NO_FATAL_FAILURE(connected.hashChunk({1}));
        connected.upstream.push(makeChunk({1}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        std::ignore = CurrentMemoryTracker::alloc(ConnectedDistinct::default_spill_threshold);
        SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(ConnectedDistinct::default_spill_threshold));
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        auto run = connected.attachRun();
        ASSERT_TRUE(run.sink);
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::Ready);
        run.sink->work();
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::Ready);
        run.sink->work();
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);

        /// Completion is signaled by closing the port, never by sending a chunk with an empty header.
        run.sink->getCompletionPort().push(Chunk{});
        try
        {
            transform.prepare();
            FAIL() << "Expected an exception for data on the completion port";
        }
        catch (const Exception & exception)
        {
            EXPECT_EQ(exception.code(), ErrorCodes::LOGICAL_ERROR);
        }
    });
}

#endif

TEST_F(ExternalDistinctTransformTest, SpillsBeforeLowCardinalityBitmapAllocation)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        const auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(type, "k")});
        constexpr size_t dictionary_size = 200000;
        auto column = type->createColumn();
        for (size_t i = 0; i < dictionary_size; ++i)
            column->insert(Field(std::to_string(i)));
        auto input = column->cut(0, 3);
        column.reset();
        ASSERT_GE(assert_cast<const ColumnLowCardinality &>(*input).getDictionary().size(), dictionary_size);
        const UInt64 threshold = query.get() + 128 * 1024;
        ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{},
            threshold, tmp_data, /*min_free_disk_space_=*/ 0,
            /*max_block_size_rows_=*/ 3, /*preferred_block_bytes_=*/ DEFAULT_BLOCK_SIZE * 256,
            /*preserve_input_order_=*/ false);
        OutputPort upstream{header};
        InputPort downstream{header};
        connect(upstream, transform.getInputs().front());
        connect(transform.getOutputs().front(), downstream);
        downstream.setNeeded();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        upstream.push(Chunk({std::move(input)}, 3));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);

        /// Spilling must start before allocating a bitmap for the dictionary retained by these
        /// three rows.
        ASSERT_NO_THROW(transform.work());
        EXPECT_FALSE(downstream.hasData());
        EXPECT_EQ(transform.prepare(), IProcessor::Status::Ready);

        /// The unconsumed input is collected into an ordinary run without constructing the LC bitmap.
        ASSERT_NO_THROW(transform.work());
        EXPECT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
    }).join();
}

TEST_F(ExternalDistinctTransformTest, SpillsBeforeFilteringWithSpareTableCapacity)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto header = std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
            ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "payload")});
        constexpr size_t num_rows = 8;
        auto value = ColumnString::create();
        value->insert(Field(String(1 << 20, 'x')));
        ColumnPtr constant = ColumnConst::create(std::move(value), num_rows);
        const Columns payloads{
            constant->convertToFullColumnIfConst(),
            constant,
            ColumnReplicated::create(assert_cast<const ColumnConst &>(*constant).getDataColumnPtr(),
                ColumnUInt8::create(num_rows, UInt8(0)))};
        for (const auto & payload : payloads)
        {
            SCOPED_TRACE(payload->getName());
            auto input = makeChunk({1, 2, 3, 4, 5, 6, 7, 7});
            input.addColumn(payload);
            size_t materialization_bytes = 0;
            size_t filtering_bytes = 0;
            {
                auto prepared = input.clone();
                materializeChunk(prepared);
                filtering_bytes = prepared.allocatedBytes();
                if (prepared.getColumns()[1] != payload)
                    materialization_bytes = prepared.getColumns()[1]->allocatedBytes();
            }
            /// The table needs no growth, but filtering would copy most of the wide payload.
            const UInt64 threshold = query.get() + materialization_bytes + filtering_bytes / 4 + 65536;
            ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"},
                threshold, tmp_data, /*min_free_disk_space_=*/ 0,
                /*max_block_size_rows_=*/ 2, /*preferred_block_bytes_=*/ DEFAULT_BLOCK_SIZE * 256,
                /*preserve_input_order_=*/ false);
            OutputPort upstream{header};
            InputPort downstream{header};
            connect(upstream, transform.getInputs().front());
            connect(transform.getOutputs().front(), downstream);
            downstream.setNeeded();
            ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
            upstream.push(std::move(input));
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            ASSERT_NO_THROW(transform.work());
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            EXPECT_FALSE(downstream.hasData());

            /// The unconsumed input is collected into an ordinary run after switching to spilling.
            ASSERT_NO_THROW(transform.work());
            EXPECT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        }
    }).join();
}

TEST_F(ExternalDistinctTransformTest, ReservesSuppressionMemoryAlongsideFilteredOutput)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto header = std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
            ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "payload")});
        constexpr UInt64 threshold = 256 << 20;
        ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"},
            threshold, tmp_data, /*min_free_disk_space_=*/ 0,
            /*max_block_size_rows_=*/ 8, /*preferred_block_bytes_=*/ DEFAULT_BLOCK_SIZE * 256,
            /*preserve_input_order_=*/ false);
        OutputPort upstream{header};
        InputPort downstream{header};
        connect(upstream, transform.getInputs().front());
        connect(transform.getOutputs().front(), downstream);
        downstream.setNeeded();

        /// Retain a key so switching to spilling must prepare a suppression run.
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        auto first = makeChunk({0});
        auto first_payload = ColumnString::create();
        first_payload->insertDefault();
        first.addColumn(std::move(first_payload));
        upstream.push(std::move(first));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        ASSERT_TRUE(downstream.hasData());
        EXPECT_EQ(downstream.pull().getNumRows(), 1);
        downstream.setNeeded();

        auto input = makeChunk({1, 2, 3, 4, 5, 6, 7, 7});
        auto payload = ColumnString::create();
        payload->insertMany(Field(String(4 << 20, 'x')), input.getNumRows());
        /// Keep the original payload alive independently of the transform. Filtering the duplicate
        /// row allocates output columns without releasing this shared input storage.
        ColumnPtr shared_payload = std::move(payload);
        input.addColumn(shared_payload);
        const size_t input_bytes = input.allocatedBytes();
        upstream.push(std::move(input));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);

        /// Either the filtering copy or suppression workspace fits on its own, but the copy can stay
        /// in the pending output while suppression starts. Reserve room for both before inserting.
        const Int64 pressure = threshold - query.get() - static_cast<Int64>(input_bytes + (16 << 20));
        ASSERT_GT(pressure, 0);
        std::ignore = CurrentMemoryTracker::alloc(pressure);
        SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(pressure));
        ASSERT_NO_THROW(transform.work());
        EXPECT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        EXPECT_FALSE(downstream.hasData());
    }).join();
}

TEST_F(ExternalDistinctTransformTest, CheckedInsertionSpillsUnprocessedSuffix)
{
    withQueryThread([&]
    {
        for (const bool ordered : {false, true})
        {
            SCOPED_TRACE(ordered);
            const auto u64 = std::make_shared<DataTypeUInt64>();
            const auto header = std::make_shared<const Block>(Block{
                ColumnWithTypeAndName(u64, "k"), ColumnWithTypeAndName(u64, "payload")});
            constexpr size_t rows = 65536;
            constexpr size_t unique_keys = 32769;
            auto keys = ColumnUInt64::create(rows);
            auto payload = ColumnUInt64::create(rows);
            for (size_t row = 0; row < rows; ++row)
            {
                keys->getData()[row] = unique_keys - 1 - row % unique_keys;
                payload->getData()[row] = row;
            }
            Chunk input(Columns{std::move(keys), std::move(payload)}, rows);
            /// Keep the source columns shared while the transform filters and cuts its input.
            auto shared_input = input.clone();
            Chunks chunks;
            chunks.push_back(std::move(input));
            auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
            constexpr UInt64 threshold = 256 << 20;
            auto transform = std::make_shared<ExternalDistinctTransform>(header, SizeLimits{}, /*limit_hint_=*/ 0,
                Names{"k"}, threshold, tmp_data, /*min_free_disk_space_=*/ 0, rows, DEFAULT_BLOCK_SIZE * 256, ordered);
            connect(source->getPort(), transform->getInputs().front());
            auto * output_port = &transform->getOutputs().front();
            auto processors = std::make_shared<Processors>();
            processors->emplace_back(std::move(source));
            processors->emplace_back(std::move(transform));
            QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
            PullingPipelineExecutor executor(pipeline);

            /// The full-chunk growth cannot fit, but checked insertion can emit a prefix before the
            /// next table growth forces its suffix into ordinary runs.
            const Int64 pressure = threshold - getCurrentQueryMemoryUsage() - (43 << 20);
            ASSERT_GT(pressure, 0);
            std::ignore = CurrentMemoryTracker::alloc(pressure);
            SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(pressure));
            Block block;
            size_t output_rows = 0;
            std::vector<bool> seen(unique_keys);
            while (executor.pull(block))
            {
                if (output_rows == 0)
                {
                    EXPECT_GT(block.rows(), 0);
                    EXPECT_LT(block.rows(), unique_keys);
                }
                for (size_t row = 0; row < block.rows(); ++row)
                {
                    const auto key = block.getByPosition(0).column->getUInt(row);
                    const auto value = block.getByPosition(1).column->getUInt(row);
                    ASSERT_LT(key, unique_keys);
                    EXPECT_FALSE(seen[key]);
                    seen[key] = true;
                    EXPECT_EQ(value, unique_keys - 1 - key);
                    if (ordered)
                        EXPECT_EQ(key, unique_keys - 1 - output_rows);
                    ++output_rows;
                }
            }
            EXPECT_EQ(output_rows, unique_keys);
        }
    });
}

TEST_F(ExternalDistinctTransformTest, SpillFilesUseByteSizedBlocks)
{
    withQueryThread([&]
    {
        for (const bool suppression : {false, true})
        for (const bool uneven : {false, true})
        for (const size_t preferred_bytes : {0, 65536})
        {
            SCOPED_TRACE(::testing::Message() << "suppression=" << suppression << ", uneven=" << uneven
                << ", preferred_bytes=" << preferred_bytes);
            const auto header = std::make_shared<const Block>(Block{
                ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "k"),
                ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload")});
            constexpr size_t rows = 512;
            auto columns = header->cloneEmptyColumns();
            Strings expected_keys;
            for (size_t row = 0; row < rows; ++row)
            {
                auto key = std::to_string(row);
                key.resize(uneven && row % 8 == 0 ? 8192 : 1024, 'x');
                expected_keys.push_back(key);
                columns[0]->insert(key);
                columns[1]->insert(UInt64(row));
            }
            std::ranges::sort(expected_keys);
            Chunk input(std::move(columns), rows);
            constexpr size_t hashing_threshold = 128 << 20;
            ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"},
                suppression ? hashing_threshold : 1, tmp_data, /*min_free_disk_space_=*/ 0,
                /*max_block_size_rows_=*/ rows, preferred_bytes, /*preserve_input_order_=*/ false);
            OutputPort upstream{header};
            InputPort downstream{header};
            connect(upstream, transform.getInputs().front());
            connect(transform.getOutputs().front(), downstream);
            downstream.setNeeded();
            ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
            upstream.push(input.clone());
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();

            /// Suppression runs contain keys already emitted during hashing. Memory used by another
            /// operator forces the next chunk to spill while the retained keys still need extraction.
            if (suppression)
            {
                ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
                ASSERT_TRUE(downstream.hasData());
                ASSERT_EQ(downstream.pull().getNumRows(), rows);
                upstream.push(input.clone());
            }
            const Int64 pressure = suppression ? hashing_threshold : 0;
            std::ignore = CurrentMemoryTracker::alloc(pressure);
            SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(pressure));
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();

            /// Inspect the serialized blocks themselves: reducing only final output sizes would not
            /// reduce the decoded block retained by each temporary-file reader.
            Blocks spilled_blocks;
            ASSERT_NO_FATAL_FAILURE(readFirstSpillRun(transform, spilled_blocks));
            Strings actual_keys;
            size_t blocks = 0;
            for (const auto & block : spilled_blocks)
            {
                EXPECT_LE(block.rows(), preferred_bytes ? 128 : rows);
                for (size_t row = 0; row < block.rows(); ++row)
                    actual_keys.emplace_back(block.getByName("k").column->getDataAt(row));
                ++blocks;
            }
            EXPECT_EQ(blocks, preferred_bytes ? 4 : 1);
            EXPECT_EQ(actual_keys, expected_keys);
        }
    });
}

TEST_F(ExternalDistinctTransformTest, CoalescedInputKeepsFirstPayload)
{
    withQueryThread([&]
    {
        const auto u64 = std::make_shared<DataTypeUInt64>();
        const DataTypes key_types{
            u64, std::make_shared<DataTypeNullable>(u64),
            std::make_shared<DataTypeArray>(u64), std::make_shared<DataTypeString>()};
        for (const auto & type : key_types)
        for (const bool ordered : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "type=" << type->getName() << ", ordered=" << ordered);
            const auto header = std::make_shared<const Block>(Block{
                ColumnWithTypeAndName(type, "k"), ColumnWithTypeAndName(u64, "payload")});
            const auto make_key = [&](UInt64 key) -> Field
            {
                if (type->getTypeId() == TypeIndex::Array)
                    return Array{key};
                if (type->isNullable() && key == 0)
                    return Null{};
                if (type->getTypeId() == TypeIndex::String)
                    return std::to_string(key) + String(key % 32 == 0 ? 8192 : 8, 'x');
                return key;
            };
            /// The wide source forces spilling while it retains most chunks, then releases enough
            /// memory for later chunks to coalesce under the larger threshold.
            const bool wide = type->getTypeId() == TypeIndex::String;
            const size_t rows = wide ? 262143 : 131071;
            const size_t threshold = wide ? 96 << 20 : 32 << 20;
            constexpr size_t unique_keys = 4093;
            constexpr size_t input_rows = 127;
            size_t metadata_merge_calls = 0;
            Chunks chunks;
            for (size_t begin = 0; begin < rows; begin += input_rows)
            {
                auto columns = header->cloneEmptyColumns();
                const size_t count = std::min(input_rows, rows - begin);
                for (size_t row = begin; row < begin + count; ++row)
                {
                    columns[0]->insert(make_key(unique_keys - 1 - row % unique_keys));
                    columns[1]->insert(UInt64(row));
                }
                chunks.emplace_back(std::move(columns), count);
                chunks.back().getChunkInfos().add(std::make_shared<MergeCountingChunkInfo>(metadata_merge_calls));
            }
            auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
            auto transform = std::make_shared<ExternalDistinctTransform>(
                header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"}, threshold,
                tmp_data, /*min_free_disk_space_=*/ 0, input_rows, /*preferred_block_bytes_=*/ 65536, ordered);
            connect(source->getPort(), transform->getInputs().front());
            auto * output_port = &transform->getOutputs().front();
            auto processors = std::make_shared<Processors>();
            processors->emplace_back(std::move(source));
            processors->emplace_back(std::move(transform));
            QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
            PullingPipelineExecutor executor(pipeline);
            Block block;
            size_t output_rows = 0;
            std::vector<bool> seen(unique_keys);
            while (executor.pull(block))
            {
                for (size_t row = 0; row < block.rows(); ++row)
                {
                    const auto payload = block.getByName("payload").column->getUInt(row);
                    ASSERT_LT(payload, unique_keys);
                    EXPECT_FALSE(seen[payload]);
                    seen[payload] = true;
                    EXPECT_EQ((*block.getByName("k").column)[row], make_key(unique_keys - 1 - payload));
                    if (ordered)
                        EXPECT_EQ(payload, output_rows);
                    ++output_rows;
                }
            }
            EXPECT_EQ(output_rows, unique_keys);
            EXPECT_EQ(metadata_merge_calls, 0);
        }
    });
}

TEST_F(ExternalDistinctTransformTest, SuppressionSortingKeepsSortEquivalentKeys)
{
    withQueryThread([&]
    {
        const auto header = std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(std::make_shared<DataTypeFloat64>(), "k")});
        /// Hashing retains distinct floating-point bit patterns. Sorting suppression keys must keep
        /// every representation even when zeros or different NaNs compare equal in the sort order.
        std::vector<UInt64> expected{0, 0x8000000000000000ULL, 0x7ff8000000000000ULL, 0x7ff8000000000001ULL};
        auto keys = ColumnFloat64::create();
        for (const auto bits : expected)
            keys->insertValue(std::bit_cast<Float64>(bits));
        Chunk input(Columns{std::move(keys)}, expected.size());
        constexpr size_t threshold = 64 << 20;
        ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{}, threshold,
            tmp_data, /*min_free_disk_space_=*/ 0, /*max_block_size_rows_=*/ 1,
            /*preferred_block_bytes_=*/ 65536, /*preserve_input_order_=*/ false);
        OutputPort upstream{header};
        InputPort downstream{header};
        connect(upstream, transform.getInputs().front());
        connect(transform.getOutputs().front(), downstream);
        downstream.setNeeded();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        upstream.push(input.clone());
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        ASSERT_TRUE(downstream.hasData());
        ASSERT_EQ(downstream.pull().getNumRows(), expected.size());
        upstream.push(std::move(input));
        std::ignore = CurrentMemoryTracker::alloc(threshold);
        SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(threshold));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        Blocks blocks;
        ASSERT_NO_FATAL_FAILURE(readFirstSpillRun(transform, blocks));
        std::vector<UInt64> actual;
        for (const auto & block : blocks)
            for (size_t row = 0; row < block.rows(); ++row)
                actual.push_back(std::bit_cast<UInt64>(block.getByName("k").column->getFloat64(row)));
        std::ranges::sort(actual);
        std::ranges::sort(expected);
        EXPECT_EQ(actual, expected);
    });
}
