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

/// Tags data chunks with their epoch. On seal chunks additionally carries the runtime filter
/// payload collected from the build side: here a [min, max] envelope, in the real version
/// possibly a set or a bloom filter. The payload is state, not data, so it lives in the
/// chunk info and the seal chunk itself is empty.
struct EpochInfo : public ChunkInfoCloneable<EpochInfo>
{
    struct Envelope
    {
        UInt64 min = 0;
        UInt64 max = 0;
    };

    explicit EpochInfo(size_t epoch_) : epoch(epoch_) {}
    EpochInfo(size_t epoch_, std::optional<Envelope> envelope_) : epoch(epoch_), envelope(envelope_), is_seal(true) {}
    EpochInfo(const EpochInfo & other) = default;

    size_t epoch;
    /// Empty optional on a seal means the epoch has no build rows: the probe side may skip it.
    std::optional<Envelope> envelope;
    bool is_seal = false;
};

Chunk makeSeal(size_t epoch, std::optional<EpochInfo::Envelope> envelope)
{
    Chunk seal(Columns{ColumnUInt64::create()}, 0);
    seal.getChunkInfos().add(std::make_shared<EpochInfo>(epoch, envelope));
    return seal;
}

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
            /// Odd epochs have no build rows at all: they must still be sealed by the
            /// collector (punctuation liveness) and skipped by the reader.
            next_epoch += 2;
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
    /// The collector knows the epoch schedule (in the real version: the planner's split
    /// points), so it can seal epochs the build side has no rows for.
    explicit EpochBuildCollector(size_t total_epochs_)
        : IProcessor({std::make_shared<const Block>(epochHeader())}, {std::make_shared<const Block>(epochHeader())})
        , total_epochs(total_epochs_)
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
            if (next_epoch_to_seal < total_epochs)
            {
                sealUpTo(total_epochs);
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

        /// Everything before the chunk's epoch is finished (build chunks arrive in epoch
        /// order), including epochs with no build rows.
        sealUpTo(info->epoch);

        const auto & data = assert_cast<const ColumnUInt64 &>(*pulled.getColumns().front()).getData();
        for (auto v : data)
        {
            if (!envelope)
                envelope = EpochInfo::Envelope{v, v};
            envelope->min = std::min(envelope->min, v);
            envelope->max = std::max(envelope->max, v);
        }
        pulled = Chunk();
    }

    std::vector<size_t> sealed_epochs;

private:
    /// Seal all epochs before `epoch`: the current one with its collected envelope,
    /// the ones the build side had no rows for with an empty payload.
    void sealUpTo(size_t epoch)
    {
        while (next_epoch_to_seal < epoch)
        {
            ready_seals.push_back(makeSeal(next_epoch_to_seal, std::exchange(envelope, std::nullopt)));
            sealed_epochs.push_back(next_epoch_to_seal);
            ++next_epoch_to_seal;
        }
    }

    const size_t total_epochs;
    Chunk pulled;
    size_t next_epoch_to_seal = 0;
    std::optional<EpochInfo::Envelope> envelope;
    std::deque<Chunk> ready_seals;
};

/// The seam the real MergeTree implementation fills: cut the epoch's ranges from the pool,
/// refine them with the seal payload (PR 1 refiner contract), read incrementally.
/// An empty chunk from readNext means the epoch is exhausted.
class IEpochReader
{
public:
    virtual ~IEpochReader() = default;
    virtual void startEpoch(size_t epoch, const EpochInfo::Envelope & envelope) = 0;
    virtual Chunk readNext() = 0;
};

class FakeEpochReader : public IEpochReader
{
public:
    explicit FakeEpochReader(size_t chunks_per_read_) : chunks_per_read(chunks_per_read_) {}

    void startEpoch(size_t epoch, const EpochInfo::Envelope & envelope) override
    {
        read_epochs.push_back(epoch);
        envelopes.push_back(envelope);
        current_epoch = epoch;
        chunks_left = chunks_per_read;
    }

    Chunk readNext() override
    {
        if (!chunks_left)
            return {};
        --chunks_left;
        return makeChunk(current_epoch, /*rows=*/ 4, /*seed=*/ chunks_per_read - chunks_left - 1);
    }

    std::vector<size_t> read_epochs;
    std::vector<EpochInfo::Envelope> envelopes;

private:
    const size_t chunks_per_read;
    size_t current_epoch = 0;
    size_t chunks_left = 0;
};

/// Consumes seal chunks and only then reads the sealed epoch through IEpochReader,
/// one chunk per work() call, emitting the data chunks.
class ReadOnSealTransform : public IProcessor
{
public:
    explicit ReadOnSealTransform(std::shared_ptr<IEpochReader> epoch_reader_)
        : IProcessor({std::make_shared<const Block>(epochHeader())}, {std::make_shared<const Block>(epochHeader())})
        , epoch_reader(std::move(epoch_reader_))
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

        if (ready_chunk)
        {
            output.push(std::move(ready_chunk));
            return Status::PortFull;
        }

        if (reading_epoch)
            return Status::Ready;

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
        if (reading_epoch)
        {
            ready_chunk = epoch_reader->readNext();
            if (!ready_chunk)
                reading_epoch = false;
            return;
        }

        chassert(seal);
        auto info = seal.getChunkInfos().get<EpochInfo>();
        chassert(info && info->is_seal);

        /// An epoch with no build rows is never read at all.
        if (info->envelope)
        {
            epoch_reader->startEpoch(info->epoch, *info->envelope);
            reading_epoch = true;
        }
        else
        {
            skipped_epochs.push_back(info->epoch);
        }

        seal = Chunk();
    }

    std::vector<size_t> skipped_epochs;

private:
    const std::shared_ptr<IEpochReader> epoch_reader;
    Chunk seal;
    Chunk ready_chunk;
    bool reading_epoch = false;
};

}

TEST(SealDrivenReading, GatingThroughPipelineEdge)
{
    constexpr size_t num_epochs = 17;
    constexpr size_t chunks_per_epoch = 3;
    constexpr size_t chunks_per_read = 2;

    auto source = std::make_shared<FakeBuildSource>(num_epochs, chunks_per_epoch);
    auto collector = std::make_shared<EpochBuildCollector>(num_epochs);
    auto epoch_reader = std::make_shared<FakeEpochReader>(chunks_per_read);
    auto reader = std::make_shared<ReadOnSealTransform>(epoch_reader);

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

    /// Every epoch was sealed exactly once, in order — including the odd ones the build
    /// side had no rows for.
    ASSERT_EQ(collector->sealed_epochs.size(), num_epochs);
    for (size_t e = 0; e < num_epochs; ++e)
        ASSERT_EQ(collector->sealed_epochs[e], e);

    /// The reader read exactly the even epochs (in order) and skipped the odd ones without
    /// reading anything.
    const size_t even_epochs = (num_epochs + 1) / 2;
    ASSERT_EQ(epoch_reader->read_epochs.size(), even_epochs);
    ASSERT_EQ(reader->skipped_epochs.size(), num_epochs - even_epochs);
    for (size_t i = 0; i < epoch_reader->read_epochs.size(); ++i)
    {
        size_t epoch = epoch_reader->read_epochs[i];
        ASSERT_EQ(epoch, i * 2);
        /// The envelope covers exactly the build values of the epoch.
        ASSERT_EQ(epoch_reader->envelopes[i].min, epoch * 1000000);
        ASSERT_GE(epoch_reader->envelopes[i].max, epoch * 1000000);
        ASSERT_LT(epoch_reader->envelopes[i].max, (epoch + 1) * 1000000);
    }
    for (size_t i = 0; i < reader->skipped_epochs.size(); ++i)
        ASSERT_EQ(reader->skipped_epochs[i], i * 2 + 1);

    /// Data arrives in epoch order, `chunks_per_read` chunks per read epoch.
    ASSERT_EQ(produced_epochs.size(), even_epochs * chunks_per_read);
    for (size_t i = 0; i < produced_epochs.size(); ++i)
        ASSERT_EQ(produced_epochs[i], (i / chunks_per_read) * 2);
}
