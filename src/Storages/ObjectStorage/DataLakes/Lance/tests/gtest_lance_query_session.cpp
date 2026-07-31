#include "config.h"

#if USE_LANCE

#include <gtest/gtest.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/tests/gtest_global_context.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <vector>

using namespace DB;

namespace ProfileEvents
{
extern const Event LanceArrowFieldMappingsBuilt;
extern const Event LanceBatchSourcesActive;
extern const Event LanceBatchesRead;
}

namespace
{
Lance::TableStateSnapshot makeSnapshot(UInt64 version, UInt8 seed)
{
    Lance::TableStateSnapshot snapshot;
    snapshot.version = version;
    snapshot.manifest_id.fill(seed);
    snapshot.manifest_size = 512;
    snapshot.manifest_sha256.fill(seed + 1);
    return snapshot;
}

std::shared_ptr<arrow::RecordBatch> makeBatch(Int64 first_value, size_t rows)
{
    arrow::Int64Builder builder;
    for (size_t index = 0; index < rows; ++index)
    {
        const auto status = builder.Append(first_value + static_cast<Int64>(index));
        if (!status.ok())
            throw std::runtime_error(status.ToString());
    }

    std::shared_ptr<arrow::Array> array;
    const auto status = builder.Finish(&array);
    if (!status.ok())
        throw std::runtime_error(status.ToString());
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int64(), /* nullable */ false)}),
        static_cast<int64_t>(rows),
        {std::move(array)});
}

Block makeInt64Header()
{
    Block header;
    auto type = std::make_shared<DataTypeInt64>();
    header.insert({type->createColumn(), type, "id"});
    return header;
}

std::shared_ptr<arrow::RecordBatch> makeInt32Batch(Int32 value)
{
    arrow::Int32Builder builder;
    if (const auto status = builder.Append(value); !status.ok())
        throw std::runtime_error(status.ToString());
    auto result = builder.Finish();
    if (!result.ok())
        throw std::runtime_error(result.status().ToString());
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int32(), /* nullable */ false)}),
        1,
        {*std::move(result)});
}

std::shared_ptr<arrow::RecordBatch> makeDuplicateFieldBatch()
{
    const auto first = makeBatch(1, 1)->column(0);
    const auto second = makeBatch(2, 1)->column(0);
    return arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::int64(), /* nullable */ false),
            arrow::field("id", arrow::int64(), /* nullable */ false),
        }),
        1,
        {first, second});
}

struct FakeBatchProviderState
{
    std::mutex mutex;
    std::condition_variable condition;
    std::set<std::thread::id> consumers;
    size_t expected_initial_consumers = 0;
    size_t initial_batches_delivered = 0;
    size_t released_batches = 0;
    bool block_until_cancel = false;
    bool receiver_waiting = false;
    bool cancelled = false;
};

class FakeBatchProvider final : public Lance::BatchProvider
{
public:
    FakeBatchProvider(
        std::shared_ptr<FakeBatchProviderState> state_, size_t batch_count, size_t rows_per_batch, bool fail_after_batches_ = false)
        : shared_state(std::move(state_))
        , fail_after_batches(fail_after_batches_)
        , projected_schema(arrow::schema({arrow::field("id", arrow::int64())}))
    {
        batches.reserve(batch_count);
        for (size_t index = 0; index < batch_count; ++index)
        {
            batches.push_back({
                .record_batch = makeBatch(static_cast<Int64>(index * rows_per_batch), rows_per_batch),
                .rows = rows_per_batch,
                .bytes = rows_per_batch * sizeof(Int64),
            });
        }
    }

    FakeBatchProvider(
        std::shared_ptr<FakeBatchProviderState> state_,
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches)
        : shared_state(std::move(state_))
        , fail_after_batches(false)
        , projected_schema(record_batches.empty() ? arrow::schema({}) : record_batches.front()->schema())
    {
        batches.reserve(record_batches.size());
        for (auto & record_batch : record_batches)
        {
            const auto rows = static_cast<UInt64>(record_batch->num_rows());
            batches.push_back({
                .record_batch = std::move(record_batch),
                .rows = rows,
                .bytes = rows * sizeof(Int64),
            });
        }
    }

    std::optional<Lance::Scan::Batch> nextBatch() override
    {
        std::unique_lock lock(shared_state->mutex);
        const bool initial_call = shared_state->consumers.insert(std::this_thread::get_id()).second;
        if (initial_call && shared_state->expected_initial_consumers != 0)
        {
            shared_state->condition.notify_all();
            shared_state->condition.wait(
                lock,
                [&] { return shared_state->consumers.size() == shared_state->expected_initial_consumers || shared_state->cancelled; });
        }

        if (shared_state->block_until_cancel)
        {
            shared_state->receiver_waiting = true;
            shared_state->condition.notify_all();
            shared_state->condition.wait(lock, [&] { return shared_state->cancelled; });
            return std::nullopt;
        }

        if (next_batch == batches.size())
        {
            if (fail_after_batches)
            {
                producer_stats.producer_error = 1;
                throw std::runtime_error("fake Lance producer error");
            }
            producer_stats.producer_eof = 1;
            return std::nullopt;
        }

        auto result = std::move(batches[next_batch++]);
        ++producer_stats.queue_pop_batches;
        if (initial_call && shared_state->expected_initial_consumers != 0)
        {
            ++shared_state->initial_batches_delivered;
            shared_state->condition.notify_all();
            shared_state->condition.wait(
                lock,
                [&]
                { return shared_state->initial_batches_delivered == shared_state->expected_initial_consumers || shared_state->cancelled; });
        }
        return result;
    }

    void releaseBatch(UInt64) noexcept override
    {
        std::lock_guard lock(shared_state->mutex);
        ++shared_state->released_batches;
    }

    void requestCancel() noexcept override
    {
        std::lock_guard lock(shared_state->mutex);
        shared_state->cancelled = true;
        producer_stats.producer_cancel = 1;
        shared_state->condition.notify_all();
    }

    const std::shared_ptr<arrow::Schema> & schema() const override { return projected_schema; }
    Lance::Scan::Stats stats() const noexcept override
    {
        std::lock_guard lock(shared_state->mutex);
        return producer_stats;
    }

private:
    std::shared_ptr<FakeBatchProviderState> shared_state;
    bool fail_after_batches;
    std::vector<Lance::Scan::Batch> batches;
    size_t next_batch = 0;
    std::shared_ptr<arrow::Schema> projected_schema;
    Lance::Scan::Stats producer_stats;
};

class FakeCountProvider final : public Lance::CountSource::Provider
{
public:
    explicit FakeCountProvider(std::optional<size_t> rows_, bool fail_ = false)
        : rows(rows_)
        , fail(fail_)
    {
    }

    std::optional<size_t> countRows() override
    {
        ++count_calls;
        if (fail)
            throw std::runtime_error("fake Lance count error");
        return rows;
    }

    void requestCancel() noexcept override
    {
        cancelled = true;
    }

    std::optional<size_t> rows;
    bool fail;
    size_t count_calls = 0;
    bool cancelled = false;
};
}

TEST(LanceQuerySession, IdentityKeyStableAndSensitiveToCredentials)
{
    Lance::DatasetOptions a{.uri = "/tmp/ds", .use_s3 = false};
    Lance::DatasetOptions b = a;
    EXPECT_EQ(a.identityKey(), b.identityKey());

    b.s3_access_key_id = "other";
    b.use_s3 = true;
    EXPECT_NE(a.identityKey(), b.identityKey());
}

TEST(LanceQuerySession, PinSnapshotRejectsConflict)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    auto session = Lance::QuerySession::get(context);
    const auto snapshot = makeSnapshot(3, 1);
    session->pinSnapshot("id1", snapshot);
    session->pinSnapshot("id1", snapshot);
    EXPECT_EQ(session->getPinnedSnapshot("id1"), snapshot);
    EXPECT_THROW(session->pinSnapshot("id1", makeSnapshot(3, 9)), Exception);
}

TEST(LanceQuerySession, GetOrOpenReusesHandleWithinSession)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto session = Lance::QuerySession::get(context);

    Lance::DatasetOptions options{.uri = "/path/that/does/not/exist/for/session/test"};
    /// Both calls fail the same way; the second must not leave a half-open entry.
    EXPECT_THROW(std::ignore = session->getOrOpen(options), Exception);
    EXPECT_EQ(session->openCount(), 0u);
}

TEST(LanceQuerySession, SessionSharedAcrossGetCalls)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    auto session1 = Lance::QuerySession::get(context);
    auto session2 = Lance::QuerySession::get(context);
    EXPECT_EQ(session1.get(), session2.get());
}

TEST(LanceScanCoordinator, ConcurrentConsumersReceiveEveryBatchExactlyOnce)
{
    constexpr size_t consumer_count = 4;
    constexpr size_t batch_count = 64;
    auto state = std::make_shared<FakeBatchProviderState>();
    state->expected_initial_consumers = consumer_count;
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, batch_count, 1), false);

    std::mutex result_mutex;
    std::vector<Int64> ids;
    std::vector<std::thread> consumers;
    for (size_t index = 0; index < consumer_count; ++index)
    {
        consumers.emplace_back(
            [&]
            {
                while (auto batch = coordinator->nextBatch())
                {
                    const auto array = std::static_pointer_cast<arrow::Int64Array>(batch->recordBatch()->column(0));
                    std::lock_guard lock(result_mutex);
                    ids.push_back(array->Value(0));
                }
            });
    }
    for (auto & consumer : consumers)
        consumer.join();

    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids.size(), batch_count);
    for (size_t index = 0; index < batch_count; ++index)
        EXPECT_EQ(ids[index], static_cast<Int64>(index));
    EXPECT_EQ(state->consumers.size(), consumer_count);
    EXPECT_EQ(state->released_batches, batch_count);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Ended);
}

TEST(LanceReadCancellation, SiblingReadsHaveIndependentHandles)
{
    auto first = std::make_shared<Lance::ReadCancellation>(nullptr);
    auto second = std::make_shared<Lance::ReadCancellation>(nullptr);

    EXPECT_NE(first->handle()->raw(), second->handle()->raw());
    first->requestCancel();
    EXPECT_NE(first->handle()->raw(), second->handle()->raw());
}

TEST(LanceScanCoordinator, ProducerErrorPropagatesOnce)
{
    constexpr size_t consumer_count = 4;
    auto state = std::make_shared<FakeBatchProviderState>();
    state->expected_initial_consumers = consumer_count;
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 0, 0, true), false);

    std::atomic_size_t exceptions = 0;
    std::vector<std::thread> consumers;
    for (size_t index = 0; index < consumer_count; ++index)
    {
        consumers.emplace_back(
            [&]
            {
                try
                {
                    std::ignore = coordinator->nextBatch();
                }
                catch (const std::runtime_error &)
                {
                    ++exceptions;
                }
            });
    }
    for (auto & consumer : consumers)
        consumer.join();

    EXPECT_EQ(exceptions, 1);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Failed);
    EXPECT_TRUE(state->cancelled);
}

TEST(LanceScanCoordinator, CancelWakesWaitingConsumer)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    state->block_until_cancel = true;
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 0, 0), false);

    std::thread consumer([&] { EXPECT_FALSE(coordinator->nextBatch().has_value()); });
    {
        std::unique_lock lock(state->mutex);
        state->condition.wait(lock, [&] { return state->receiver_waiting; });
    }
    coordinator->cancel();
    consumer.join();

    EXPECT_TRUE(state->cancelled);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Cancelled);
}

TEST(LanceScanCoordinator, DestructorCancelsProvider)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    {
        auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 1, 1), false);
    }
    EXPECT_TRUE(state->cancelled);
}

TEST(LanceScanCoordinator, GlobalLimitSlicesLastBatch)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 1, 8), false, 3);

    auto batch = coordinator->nextBatch();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->rows(), 3);
    EXPECT_EQ(batch->recordBatch()->num_rows(), 3);
    batch.reset();
    EXPECT_EQ(state->released_batches, 1);
    EXPECT_TRUE(state->cancelled);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Ended);
    EXPECT_FALSE(coordinator->nextBatch().has_value());
}

TEST(LanceScanCoordinator, NoLimitConsumesCompleteBatch)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 1, 8), false);

    auto batch = coordinator->nextBatch();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->rows(), 8);
    batch.reset();
    EXPECT_FALSE(coordinator->nextBatch().has_value());
    EXPECT_FALSE(state->cancelled);
}

TEST(LanceScanCoordinator, EmptyProviderEndsCleanly)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 0, 0), false);

    EXPECT_FALSE(coordinator->nextBatch().has_value());
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Ended);
}

TEST(LanceBatchSource, BuildsOneFieldMappingPerActiveSource)
{
    constexpr size_t batch_count = 4;
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(
        std::make_unique<FakeBatchProvider>(state, batch_count, 1), false);
    const auto header = makeInt64Header();
    Lance::BatchSource first(header, header, coordinator, {}, {}, nullptr, FormatSettings{});
    Lance::BatchSource second(header, header, coordinator, {}, {}, nullptr, FormatSettings{});

    const auto mappings_before = CurrentThread::getProfileEvents()[ProfileEvents::LanceArrowFieldMappingsBuilt];
    const auto active_before = CurrentThread::getProfileEvents()[ProfileEvents::LanceBatchSourcesActive];
    const auto batches_before = CurrentThread::getProfileEvents()[ProfileEvents::LanceBatchesRead];

    EXPECT_TRUE(first.generate());
    EXPECT_TRUE(second.generate());
    EXPECT_TRUE(first.generate());
    EXPECT_TRUE(second.generate());
    EXPECT_FALSE(first.generate());
    EXPECT_FALSE(second.generate());

    EXPECT_EQ(CurrentThread::getProfileEvents()[ProfileEvents::LanceArrowFieldMappingsBuilt] - mappings_before, 2);
    EXPECT_EQ(CurrentThread::getProfileEvents()[ProfileEvents::LanceBatchSourcesActive] - active_before, 2);
    EXPECT_EQ(CurrentThread::getProfileEvents()[ProfileEvents::LanceBatchesRead] - batches_before, batch_count);
}

TEST(LanceBatchSource, SchemaMismatchCancelsCoordinatorAndPropagates)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches{makeBatch(1, 1), makeInt32Batch(2)};
    auto coordinator = Lance::ScanCoordinator::createWithProvider(
        std::make_unique<FakeBatchProvider>(state, std::move(batches)), false);
    const auto header = makeInt64Header();
    Lance::BatchSource source(header, header, coordinator, {}, {}, nullptr, FormatSettings{});

    EXPECT_TRUE(source.generate());
    EXPECT_THROW(std::ignore = source.generate(), Exception);
    EXPECT_TRUE(state->cancelled);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Cancelled);
}

TEST(LanceBatchSource, DuplicateFieldsCancelCoordinatorAndPropagate)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches{makeDuplicateFieldBatch()};
    auto coordinator = Lance::ScanCoordinator::createWithProvider(
        std::make_unique<FakeBatchProvider>(state, std::move(batches)), false);
    const auto header = makeInt64Header();
    Lance::BatchSource source(header, header, coordinator, {}, {}, nullptr, FormatSettings{});

    EXPECT_THROW(std::ignore = source.generate(), Exception);
    EXPECT_TRUE(state->cancelled);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Cancelled);
}

class LanceCountSource : public testing::TestWithParam<std::tuple<size_t, size_t, std::vector<size_t>>>
{
};

TEST_P(LanceCountSource, EmitsBoundedZeroColumnChunksAndCountsOnce)
{
    const auto [rows, max_block_size, expected_chunk_rows] = GetParam();
    auto provider = std::make_unique<FakeCountProvider>(rows);
    auto * provider_ptr = provider.get();
    Lance::CountSource source(Block{}, std::move(provider), max_block_size);

    std::vector<size_t> chunk_rows;
    while (true)
    {
        auto chunk = source.generate();
        if (!chunk)
            break;
        EXPECT_EQ(chunk.getNumColumns(), 0);
        chunk_rows.push_back(chunk.getNumRows());
    }

    EXPECT_EQ(chunk_rows, expected_chunk_rows);
    EXPECT_EQ(provider_ptr->count_calls, 1);
    EXPECT_FALSE(provider_ptr->cancelled);
}

INSTANTIATE_TEST_SUITE_P(
    Boundaries,
    LanceCountSource,
    testing::Values(
        std::tuple<size_t, size_t, std::vector<size_t>>{0, 4, {}},
        std::tuple<size_t, size_t, std::vector<size_t>>{1, 4, {1}},
        std::tuple<size_t, size_t, std::vector<size_t>>{4, 4, {4}},
        std::tuple<size_t, size_t, std::vector<size_t>>{9, 4, {4, 4, 1}}));

TEST(LanceCountSource, RejectsZeroMaxBlockSize)
{
    EXPECT_THROW(
        Lance::CountSource(Block{}, std::make_unique<FakeCountProvider>(1), 0),
        Exception);
}

TEST(LanceCountSource, EmitsDefaultPhysicalColumns)
{
    const auto header = makeInt64Header();
    Lance::CountSource source(header, std::make_unique<FakeCountProvider>(3), 2);

    auto first = source.generate();
    ASSERT_TRUE(first);
    ASSERT_EQ(first.getNumColumns(), 1);
    ASSERT_EQ(first.getNumRows(), 2);
    EXPECT_EQ(first.getColumns().front()->getInt(0), 0);
    EXPECT_EQ(first.getColumns().front()->getInt(1), 0);

    auto second = source.generate();
    ASSERT_TRUE(second);
    ASSERT_EQ(second.getNumColumns(), 1);
    ASSERT_EQ(second.getNumRows(), 1);
    EXPECT_EQ(second.getColumns().front()->getInt(0), 0);
    EXPECT_FALSE(source.generate());
}

TEST(LanceCountSource, CountFailureCancelsAndPropagates)
{
    auto provider = std::make_unique<FakeCountProvider>(std::nullopt, true);
    auto * provider_ptr = provider.get();
    Lance::CountSource source(Block{}, std::move(provider), 1);
    EXPECT_THROW(std::ignore = source.generate(), std::runtime_error);
    EXPECT_EQ(provider_ptr->count_calls, 1);
    EXPECT_TRUE(provider_ptr->cancelled);
}

#endif
