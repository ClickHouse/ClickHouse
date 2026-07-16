#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

/// Prototype of seal-driven reading for range-gated collocated JOINs.
///
/// The build side is consumed by a collector which emits one small "seal" chunk per finished
/// epoch (a PK range in the real setup). The probe-side reader consumes seals as its ordinary
/// data input and only then reads the corresponding epoch: gating is expressed as a pipeline
/// edge which the executor can see, there is no Status::Async and no side-channel wakeups.

namespace
{

struct EpochInfo : public ChunkInfoCloneable<EpochInfo>
{
    explicit EpochInfo(size_t epoch_) : epoch(epoch_) {}
    EpochInfo(const EpochInfo & other) = default;
    size_t epoch;
};

Block epochHeader()
{
    return Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")};
}

Chunk makeChunk(size_t epoch, size_t rows, size_t seed)
{
    auto col = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        col->insertValue(epoch * 1000000 + seed * 1000 + i);
    Chunk chunk(Columns{std::move(col)}, rows);
    chunk.getChunkInfos().add(std::make_shared<EpochInfo>(epoch));
    return chunk;
}

/// Emits `chunks_per_epoch` build chunks for each of `num_epochs` epochs, tagged with EpochInfo.
class FakeBuildSource : public ISource
{
public:
    FakeBuildSource(size_t num_epochs_, size_t chunks_per_epoch_)
        : ISource(std::make_shared<const Block>(epochHeader())), num_epochs(num_epochs_), chunks_per_epoch(chunks_per_epoch_)
    {
    }

    String getName() const override { return "FakeBuildSource"; }

protected:
    Chunk generate() override
    {
        if (next_epoch >= num_epochs)
            return {};

        auto chunk = makeChunk(next_epoch, /*rows=*/ 8, next_chunk);
        if (++next_chunk == chunks_per_epoch)
        {
            next_chunk = 0;
            ++next_epoch;
        }
        return chunk;
    }

private:
    const size_t num_epochs;
    const size_t chunks_per_epoch;
    size_t next_epoch = 0;
    size_t next_chunk = 0;
};

/// Consumes epoch-tagged build chunks ("fills the join state") and emits one seal chunk per
/// finished epoch. A seal is a single-row chunk tagged with EpochInfo; in the real version it
/// carries the runtime filter payload (min/max envelope, optionally the exact key set).
class EpochBuildCollector : public IProcessor
{
public:
    EpochBuildCollector()
        : IProcessor({std::make_shared<const Block>(epochHeader())}, {std::make_shared<const Block>(epochHeader())})
    {
    }

    String getName() const override { return "EpochBuildCollector"; }

    Status prepare() override
    {
        auto & input = inputs.front();
        auto & output = outputs.front();

        if (output.isFinished())
        {
            input.close();
            return Status::Finished;
        }

        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        if (!ready_seals.empty())
        {
            output.push(std::move(ready_seals.front()));
            ready_seals.pop_front();
            return Status::PortFull;
        }

        if (input.isFinished())
        {
            if (current_epoch)
            {
                sealCurrentEpoch();
                return Status::Ready;
            }
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        pulled = input.pull();
        return Status::Ready;
    }

    void work() override
    {
        if (!pulled)
            return;

        auto info = pulled.getChunkInfos().get<EpochInfo>();
        chassert(info);

        if (current_epoch && *current_epoch != info->epoch)
            sealCurrentEpoch();

        current_epoch = info->epoch;
        rows_ingested += pulled.getNumRows();
        pulled = Chunk();
    }

    std::vector<size_t> sealed_epochs;

private:
    void sealCurrentEpoch()
    {
        auto seal = makeChunk(*current_epoch, /*rows=*/ 1, /*seed=*/ 0);
        sealed_epochs.push_back(*current_epoch);
        ready_seals.push_back(std::move(seal));
        current_epoch.reset();
        rows_ingested = 0;
    }

    Chunk pulled;
    std::optional<size_t> current_epoch;
    size_t rows_ingested = 0;
    std::deque<Chunk> ready_seals;
};

/// Consumes seal chunks and only then "reads" the sealed epoch (a fake stand-in for cutting,
/// refining and reading the epoch ranges from a MergeTree read pool), emitting the data chunks.
class ReadOnSealTransform : public IProcessor
{
public:
    explicit ReadOnSealTransform(size_t chunks_per_read_)
        : IProcessor({std::make_shared<const Block>(epochHeader())}, {std::make_shared<const Block>(epochHeader())})
        , chunks_per_read(chunks_per_read_)
    {
    }

    String getName() const override { return "ReadOnSealTransform"; }

    Status prepare() override
    {
        auto & input = inputs.front();
        auto & output = outputs.front();

        if (output.isFinished())
        {
            input.close();
            return Status::Finished;
        }

        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        if (!ready_data.empty())
        {
            output.push(std::move(ready_data.front()));
            ready_data.pop_front();
            return Status::PortFull;
        }

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        seal = input.pull();
        return Status::Ready;
    }

    void work() override
    {
        if (!seal)
            return;

        auto info = seal.getChunkInfos().get<EpochInfo>();
        chassert(info);
        read_epochs.push_back(info->epoch);

        /// The "read": in the real version this cuts + refines + reads epoch ranges from the pool.
        for (size_t i = 0; i < chunks_per_read; ++i)
            ready_data.push_back(makeChunk(info->epoch, /*rows=*/ 4, /*seed=*/ i));

        seal = Chunk();
    }

    std::vector<size_t> read_epochs;

private:
    const size_t chunks_per_read;
    Chunk seal;
    std::deque<Chunk> ready_data;
};

}

TEST(SealDrivenReading, GatingThroughPipelineEdge)
{
    constexpr size_t num_epochs = 17;
    constexpr size_t chunks_per_epoch = 3;
    constexpr size_t chunks_per_read = 2;

    auto source = std::make_shared<FakeBuildSource>(num_epochs, chunks_per_epoch);
    auto collector = std::make_shared<EpochBuildCollector>();
    auto reader = std::make_shared<ReadOnSealTransform>(chunks_per_read);

    auto pipe = Pipe(source);
    pipe.addTransform(collector);
    pipe.addTransform(reader);

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    std::vector<size_t> produced_epochs;
    Chunk chunk;
    while (executor.pull(chunk))
    {
        if (!chunk)
            continue;
        auto info = chunk.getChunkInfos().get<EpochInfo>();
        ASSERT_TRUE(info);
        produced_epochs.push_back(info->epoch);
        ASSERT_EQ(chunk.getNumRows(), 4);
    }

    /// Every epoch was sealed exactly once, in order.
    ASSERT_EQ(collector->sealed_epochs.size(), num_epochs);
    /// The reader saw seals in order and read every epoch exactly once.
    ASSERT_EQ(reader->read_epochs, collector->sealed_epochs);
    for (size_t e = 0; e < num_epochs; ++e)
        ASSERT_EQ(reader->read_epochs[e], e);

    /// Data arrives in epoch order, `chunks_per_read` chunks per epoch.
    ASSERT_EQ(produced_epochs.size(), num_epochs * chunks_per_read);
    for (size_t i = 0; i < produced_epochs.size(); ++i)
        ASSERT_EQ(produced_epochs[i], i / chunks_per_read);
}
