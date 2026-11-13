#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <QueryPipeline/DistributedTopKCoordinator.h>
#include <Common/Exception.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <poll.h>
#endif

#include <atomic>
#include <chrono>
#include <future>
#include <initializer_list>
#include <numeric>
#include <stdexcept>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int QUERY_WAS_CANCELLED;
    extern const int UNKNOWN_PROTOCOL;
}

using namespace DB;

namespace
{

Block makeCandidates(std::initializer_list<std::pair<UInt64, Int64>> values, bool include_secondary = false)
{
    auto primary = ColumnUInt64::create();
    auto secondary = ColumnInt64::create();
    for (const auto & [first, second] : values)
    {
        primary->insertValue(first);
        secondary->insertValue(second);
    }

    Block result;
    result.insert(ColumnWithTypeAndName(std::move(primary), std::make_shared<DataTypeUInt64>(), "primary"));
    if (include_secondary)
        result.insert(ColumnWithTypeAndName(std::move(secondary), std::make_shared<DataTypeInt64>(), "secondary"));
    return result;
}

Block makeSequentialCandidates(size_t rows)
{
    auto primary = ColumnUInt64::create();
    auto & data = primary->getData();
    data.resize(rows);
    std::iota(data.begin(), data.end(), 0);

    Block result;
    result.insert(ColumnWithTypeAndName(std::move(primary), std::make_shared<DataTypeUInt64>(), "primary"));
    return result;
}

QueryCoordinationRequest makeRequest(UInt64 request_id, Block candidates)
{
    return QueryCoordinationRequest{
        .request_id = request_id,
        .kind = QueryCoordinationRequestKind::DistributedTopKCandidates,
        .payload = std::move(candidates),
    };
}

SortDescription ascendingPrimary()
{
    SortDescription description;
    description.emplace_back("primary", 1, 1);
    return description;
}

DistributedTopKCoordinator::Settings makeSettings(size_t shards, UInt64 limit, SortDescription description)
{
    Block candidate_header;
    for (const auto & key : description)
    {
        if (candidate_header.has(key.column_name))
            continue;
        if (key.column_name == "primary")
            candidate_header.insert(
                ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), key.column_name));
        else if (key.column_name == "secondary")
            candidate_header.insert(
                ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), key.column_name));
    }

    return DistributedTopKCoordinator::Settings{
        .logical_shards = shards,
        .limit = limit,
        .sort_description = std::move(description),
        .candidate_header = std::move(candidate_header),
    };
}

String serializeRequest(const QueryCoordinationRequest & request)
{
    WriteBufferFromOwnString out;
    request.serialize(out, DBMS_TCP_PROTOCOL_VERSION);
    return std::move(out.str());
}

QueryCoordinationRequest deserializeRequest(const String & serialized)
{
    ReadBufferFromString in(serialized);
    auto request = QueryCoordinationRequest::deserialize(in, DBMS_TCP_PROTOCOL_VERSION);
    if (!in.eof())
        throw std::runtime_error("Trailing request data");
    return request;
}

String serializeResponse(const QueryCoordinationResponse & response)
{
    WriteBufferFromOwnString out;
    response.serialize(out);
    return std::move(out.str());
}

QueryCoordinationResponse deserializeResponse(const String & serialized, size_t candidate_rows)
{
    ReadBufferFromString in(serialized);
    auto response = QueryCoordinationResponse::deserialize(in, candidate_rows);
    if (!in.eof())
        throw std::runtime_error("Trailing response data");
    return response;
}

}

TEST(QueryCoordinationRequest, RoundTripsCandidates)
{
    auto request = makeRequest(7, makeCandidates({{1, 0}, {2, 0}}));
    auto deserialized = deserializeRequest(serializeRequest(request));

    EXPECT_EQ(deserialized.request_id, 7u);
    EXPECT_EQ(deserialized.mode, QueryCoordinationRequestMode::Candidates);
    EXPECT_EQ(deserialized.payload.rows(), 2u);
    EXPECT_EQ(deserialized.payload.columns(), 1u);
}

TEST(QueryCoordinationRequest, RoundTripsMoreThanOneMillionRows)
{
    constexpr size_t rows = 1'000'001;
    auto candidates = ColumnUInt8::create();
    candidates->getData().resize(rows);

    Block payload;
    payload.insert(ColumnWithTypeAndName(std::move(candidates), std::make_shared<DataTypeUInt8>(), "key"));

    auto request = makeRequest(8, std::move(payload));
    auto serialized = serializeRequest(request);
    request.payload.clear();
    auto deserialized = deserializeRequest(serialized);
    EXPECT_EQ(deserialized.request_id, 8u);
    EXPECT_EQ(deserialized.mode, QueryCoordinationRequestMode::Candidates);
    EXPECT_EQ(deserialized.payload.rows(), rows);
    EXPECT_EQ(deserialized.payload.columns(), 1u);
}

TEST(QueryCoordinationRequest, RoundTripsFallbackWithoutPayload)
{
    QueryCoordinationRequest request{
        .request_id = 9,
        .kind = QueryCoordinationRequestKind::DistributedTopKCandidates,
        .mode = QueryCoordinationRequestMode::FallbackAll,
        .payload = {},
    };
    auto serialized = serializeRequest(request);
    EXPECT_EQ(serialized.size(), 4u);

    auto deserialized = deserializeRequest(serialized);
    EXPECT_EQ(deserialized.request_id, 9u);
    EXPECT_EQ(deserialized.mode, QueryCoordinationRequestMode::FallbackAll);
    EXPECT_EQ(deserialized.payload.columns(), 0u);
}

TEST(QueryCoordinationRequest, RejectsCandidateWithoutPayload)
{
    WriteBufferFromOwnString candidates_out;
    writeVarUInt(10, candidates_out);
    writeVarUInt(static_cast<UInt64>(QueryCoordinationRequestKind::DistributedTopKCandidates), candidates_out);
    writeVarUInt(QueryCoordinationRequest::CURRENT_VERSION, candidates_out);
    writeVarUInt(static_cast<UInt64>(QueryCoordinationRequestMode::Candidates), candidates_out);
    EXPECT_THROW(deserializeRequest(candidates_out.str()), Exception);
}

TEST(QueryCoordinationRequest, RejectsFutureVersionBeforePayload)
{
    WriteBufferFromOwnString out;
    writeVarUInt(11, out);
    writeVarUInt(static_cast<UInt64>(QueryCoordinationRequestKind::DistributedTopKCandidates), out);
    writeVarUInt(QueryCoordinationRequest::CURRENT_VERSION + 1, out);
    try
    {
        deserializeRequest(out.str());
        FAIL() << "Future coordination version was accepted";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::UNKNOWN_PROTOCOL);
    }
}

TEST(QueryCoordinationResponse, BoundsSelectedOrdinalsByCandidateRows)
{
    QueryCoordinationResponse response{
        .request_id = 12,
        .mode = QueryCoordinationResponseMode::Selected,
        .selected_ordinals = {0, 2},
    };
    auto deserialized = deserializeResponse(serializeResponse(response), 3);
    EXPECT_EQ(deserialized.selected_ordinals, (std::vector<UInt64>{0, 2}));

    WriteBufferFromOwnString excessive_count;
    writeVarUInt(12, excessive_count);
    writeVarUInt(static_cast<UInt64>(QueryCoordinationResponseMode::Selected), excessive_count);
    writeVarUInt(4, excessive_count);
    try
    {
        deserializeResponse(excessive_count.str(), 3);
        FAIL() << "Excessive ordinal count was accepted";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::INCORRECT_DATA);
    }

    response.selected_ordinals = {3};
    EXPECT_THROW(deserializeResponse(serializeResponse(response), 3), Exception);
}

TEST(QueryCoordinationResponse, AllowsEmptyFallbackForZeroCandidates)
{
    QueryCoordinationResponse response{
        .request_id = 13,
        .mode = QueryCoordinationResponseMode::FallbackAll,
        .selected_ordinals = {},
    };
    auto deserialized = deserializeResponse(serializeResponse(response), 0);
    EXPECT_EQ(deserialized.mode, QueryCoordinationResponseMode::FallbackAll);
    EXPECT_TRUE(deserialized.selected_ordinals.empty());
}

TEST(DistributedTopKCoordinator, MergesInterleavedCandidatesWithEmptyShard)
{
    DistributedTopKCoordinator coordinator(makeSettings(3, 4, ascendingPrimary()));

    EXPECT_FALSE(coordinator.submit(0, makeRequest(10, makeCandidates({}))));
    EXPECT_FALSE(coordinator.submit(1, makeRequest(11, makeCandidates({{1, 0}, {4, 0}, {7, 0}}))));
    EXPECT_TRUE(coordinator.submit(2, makeRequest(12, makeCandidates({{2, 0}, {3, 0}, {8, 0}}))));

    const auto empty = coordinator.takeResponse(0);
    const auto first = coordinator.takeResponse(1);
    const auto second = coordinator.takeResponse(2);

    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::Selected);
    EXPECT_EQ(empty.request_id, 10u);
    EXPECT_TRUE(empty.selected_ordinals.empty());
    EXPECT_EQ(first.selected_ordinals, (std::vector<UInt64>{0, 1}));
    EXPECT_EQ(second.selected_ordinals, (std::vector<UInt64>{0, 1}));
}

TEST(DistributedTopKCoordinator, HonorsCompoundOrdering)
{
    SortDescription description;
    description.emplace_back("primary", 1, 1);
    description.emplace_back("secondary", -1, -1);
    DistributedTopKCoordinator coordinator(makeSettings(2, 4, std::move(description)));

    EXPECT_FALSE(coordinator.submit(0, makeRequest(20, makeCandidates({{1, 9}, {2, 4}, {3, 8}}, true))));
    EXPECT_TRUE(coordinator.submit(1, makeRequest(21, makeCandidates({{1, 7}, {2, 6}, {4, 9}}, true))));

    EXPECT_EQ(coordinator.takeResponse(0).selected_ordinals, (std::vector<UInt64>{0, 1}));
    EXPECT_EQ(coordinator.takeResponse(1).selected_ordinals, (std::vector<UInt64>{0, 1}));
}

TEST(DistributedTopKCoordinator, ConcurrentSubmissionsElectOneSelector)
{
    constexpr size_t shard_count = 4;
    auto settings = makeSettings(shard_count, 3, ascendingPrimary());
    std::atomic<size_t> selection_count = 0;
    settings.selection_hook = [&] { ++selection_count; };
    DistributedTopKCoordinator coordinator(std::move(settings));

    std::promise<void> start;
    auto start_future = start.get_future().share();
    std::vector<std::future<bool>> submissions;
    submissions.reserve(shard_count);
    for (size_t participant = 0; participant < shard_count; ++participant)
    {
        submissions.emplace_back(std::async(std::launch::async, [&, participant]
        {
            start_future.wait();
            return coordinator.submit(
                participant,
                makeRequest(100 + participant, makeCandidates({{participant + 1, 0}})));
        }));
    }

    start.set_value();
    size_t ready_responses = 0;
    for (auto & submission : submissions)
        ready_responses += submission.get();

    EXPECT_EQ(selection_count.load(), 1u);
    EXPECT_EQ(ready_responses, 1u);
    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::Selected);
    for (size_t participant = 0; participant < shard_count; ++participant)
    {
        const auto response = coordinator.takeResponse(participant);
        EXPECT_EQ(response.request_id, 100u + participant);
        EXPECT_EQ(response.selected_ordinals, participant < 3 ? std::vector<UInt64>{0} : std::vector<UInt64>{});
    }
}

TEST(DistributedTopKCoordinator, FallsBackForUnsupportedParticipant)
{
    DistributedTopKCoordinator coordinator(makeSettings(2, 2, ascendingPrimary()));

    EXPECT_FALSE(coordinator.submit(0, makeRequest(30, makeCandidates({{1, 0}, {2, 0}}))));
    coordinator.markParticipantUnsupported(1);

    const auto response = coordinator.takeResponse(0);
    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::FallbackAll);
    EXPECT_EQ(response.request_id, 30u);
    EXPECT_EQ(response.mode, QueryCoordinationResponseMode::FallbackAll);
    EXPECT_TRUE(response.selected_ordinals.empty());
}

TEST(DistributedTopKCoordinator, FallsBackForResourceAnnouncement)
{
    DistributedTopKCoordinator coordinator(makeSettings(3, 2, ascendingPrimary()));

    EXPECT_FALSE(coordinator.submit(0, makeRequest(35, makeCandidates({{1, 0}, {2, 0}}))));
    EXPECT_TRUE(coordinator.submit(
        1,
        QueryCoordinationRequest{
            .request_id = 36,
            .kind = QueryCoordinationRequestKind::DistributedTopKCandidates,
            .mode = QueryCoordinationRequestMode::FallbackAll,
            .payload = {},
        }));
    EXPECT_TRUE(coordinator.submit(2, makeRequest(37, makeCandidates({{3, 0}}))));

    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::FallbackAll);
    EXPECT_EQ(coordinator.takeResponse(0).mode, QueryCoordinationResponseMode::FallbackAll);
    EXPECT_EQ(coordinator.takeResponse(1).mode, QueryCoordinationResponseMode::FallbackAll);
    EXPECT_EQ(coordinator.takeResponse(2).mode, QueryCoordinationResponseMode::FallbackAll);
    EXPECT_THROW(coordinator.submit(1, makeRequest(38, makeCandidates({{4, 0}}))), Exception);
}

TEST(DistributedTopKCoordinator, RejectsFallbackAnnouncementWithPayload)
{
    DistributedTopKCoordinator coordinator(makeSettings(1, 1, ascendingPrimary()));
    auto request = makeRequest(37, makeCandidates({{1, 0}}));
    request.mode = QueryCoordinationRequestMode::FallbackAll;

    EXPECT_THROW(coordinator.submit(0, std::move(request)), Exception);
    EXPECT_FALSE(coordinator.hasSubmitted(0));
}

TEST(DistributedTopKCoordinator, FallsBackWhenShardCandidateLimitIsExceeded)
{
    DistributedTopKCoordinator coordinator(makeSettings(1, 2, ascendingPrimary()));
    EXPECT_TRUE(coordinator.submit(0, makeRequest(43, makeCandidates({{1, 0}, {2, 0}, {3, 0}}))));
    EXPECT_EQ(coordinator.takeResponse(0).mode, QueryCoordinationResponseMode::FallbackAll);
}

TEST(DistributedTopKCoordinator, CancellationSignalsAsyncWaitersAndPreservesException)
{
    DistributedTopKCoordinator coordinator(makeSettings(2, 2, ascendingPrimary()));
    EXPECT_FALSE(coordinator.submit(0, makeRequest(50, makeCandidates({{1, 0}}))));

#if defined(OS_LINUX) || defined(OS_DARWIN)
    pollfd response_event{coordinator.getResponseFileDescriptor(0), POLLIN, 0};
    ASSERT_GE(response_event.fd, 0);
    EXPECT_EQ(poll(&response_event, 1, 0), 0);
#else
    EXPECT_EQ(coordinator.getResponseFileDescriptor(0), -1);
#endif

    coordinator.cancel(std::make_exception_ptr(std::runtime_error("coordinator cancelled")));

#if defined(OS_LINUX) || defined(OS_DARWIN)
    EXPECT_EQ(poll(&response_event, 1, 0), 1);
    EXPECT_TRUE(response_event.revents & POLLIN);
#endif
    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::Cancelled);
    EXPECT_THROW(coordinator.takeResponse(0), std::runtime_error);
    EXPECT_THROW(coordinator.submit(1, makeRequest(51, makeCandidates({{2, 0}}))), std::runtime_error);
}

TEST(DistributedTopKCoordinator, CancellationInterruptsCandidateValidation)
{
    constexpr size_t rows = 1'000'000;
    constexpr size_t pause_check = 10;
    std::promise<void> validation_paused;
    auto validation_paused_future = validation_paused.get_future();
    std::promise<void> release_validation;
    auto release_validation_future = release_validation.get_future().share();
    std::atomic<size_t> validation_checks = 0;

    auto settings = makeSettings(1, rows, ascendingPrimary());
    settings.candidate_validation_hook = [&]
    {
        if (validation_checks.fetch_add(1) == pause_check)
        {
            validation_paused.set_value();
            release_validation_future.wait();
        }
    };
    DistributedTopKCoordinator coordinator(std::move(settings));

    auto submitter = std::async(std::launch::async, [&]
    {
        return coordinator.submit(0, makeRequest(60, makeSequentialCandidates(rows)));
    });
    const auto validation_status = validation_paused_future.wait_for(std::chrono::seconds(5));
    if (validation_status != std::future_status::ready)
    {
        release_validation.set_value();
        submitter.wait();
        FAIL() << "Candidate validation did not reach the cancellation point";
    }

    coordinator.cancel(std::make_exception_ptr(std::runtime_error("cancelled during validation")));
    release_validation.set_value();
    try
    {
        static_cast<void>(submitter.get());
        FAIL() << "Candidate validation ignored cancellation";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::QUERY_WAS_CANCELLED);
    }
    EXPECT_EQ(validation_checks.load(), pause_check + 1);
    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::Cancelled);
}

TEST(DistributedTopKCoordinator, CancellationInterruptsLongRunningMergeWithoutPublishing)
{
    constexpr size_t rows = 1'000'000;
    constexpr UInt64 pause_iteration = 1'024;
    std::promise<void> merge_paused;
    auto merge_paused_future = merge_paused.get_future();
    std::promise<void> release_merge;
    auto release_merge_future = release_merge.get_future().share();
    std::atomic<UInt64> iterations = 0;

    auto settings = makeSettings(1, rows, ascendingPrimary());
    settings.selection_iteration_hook = [&](UInt64 iteration)
    {
        iterations.store(iteration + 1);
        if (iteration == pause_iteration)
        {
            merge_paused.set_value();
            release_merge_future.wait();
        }
    };
    DistributedTopKCoordinator coordinator(std::move(settings));

    auto selector = std::async(std::launch::async, [&]
    {
        return coordinator.submit(0, makeRequest(60, makeSequentialCandidates(rows)));
    });
    const auto merge_status = merge_paused_future.wait_for(std::chrono::seconds(5));
    if (merge_status != std::future_status::ready)
    {
        release_merge.set_value();
        selector.wait();
        FAIL() << "Selection did not reach the cancellation point";
    }
    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::Selecting);

    coordinator.cancel(std::make_exception_ptr(std::runtime_error("cancelled during selection")));
    EXPECT_EQ(coordinator.getState(), DistributedTopKCoordinator::State::Cancelled);

    release_merge.set_value();
    EXPECT_THROW(static_cast<void>(selector.get()), std::runtime_error);
    EXPECT_EQ(iterations.load(), pause_iteration + 1);
    EXPECT_THROW(coordinator.takeResponse(0), std::runtime_error);
}
