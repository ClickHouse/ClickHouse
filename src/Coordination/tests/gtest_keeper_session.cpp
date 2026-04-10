#include "config.h"

#if USE_NURAFT

#include <Coordination/KeeperSession.h>
#include <Coordination/KeeperSessionRegistry.h>
#include <Coordination/KeeperRequestsQueue.h>
#include <Coordination/SessionRequest.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

using namespace DB;
using namespace Coordination;

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool quorum_reads;
    extern const CoordinationSettingsUInt64 max_session_active_requests;
}

namespace
{

/// Helper to create a write request with the given xid.
ZooKeeperRequestPtr makeWriteRequest(XID xid)
{
    auto req = std::make_shared<ZooKeeperCreateRequest>();
    req->xid = xid;
    req->path = "/test";
    return req;
}

/// Helper to create a read request with the given xid.
ZooKeeperRequestPtr makeReadRequest(XID xid)
{
    auto req = std::make_shared<ZooKeeperGetRequest>();
    req->xid = xid;
    req->path = "/test";
    return req;
}

/// Test fixture that creates a KeeperSession with controllable callbacks.
///
/// In the new design:
///   - `local_read` fills `req->response` directly (no response_router).
///   - `onRaftResponse` just attaches the response to the FIFO head (no pop/deliver).
///   - `onRaftCommitted` calls `drainCompleted` which pops, delivers, and advances.
///   - `failSession` terminates the session (orphans remaining requests).
///   - Quorum requests are pushed to `test_queue` by `addRequest`.
///   - `submitOne()` pulls one request from `test_queue` to simulate `requestThread`.
class KeeperSessionTest : public ::testing::Test
{
protected:
    struct LocalRead
    {
        SessionRequestPtr req;
    };

    /// Wrap a ZooKeeperRequestPtr into a SessionRequestPtr for addRequest.
    SessionRequestPtr makeKeeperReq(ZooKeeperRequestPtr request)
    {
        using namespace std::chrono;
        auto req = std::make_shared<SessionRequest>();
        req->session_id = 1;
        req->cached_bytes_size = request->bytesSize();
        req->request = std::move(request);
        req->time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        return req;
    }

    /// Default local read callback that fills responses synchronously.
    KeeperSession::LocalReadCallback defaultLocalReadCallback()
    {
        return [this](std::span<SessionRequestPtr> batch)
        {
            ++batch_call_count;
            batch_sizes.push_back(batch.size());
            for (auto & req : batch)
            {
                local_reads.push_back({req});
                auto response = req->request->makeResponse();
                response->xid = req->request->xid;
                response->zxid = 0;
                response->error = Coordination::Error::ZOK;
                req->response = response;
            }
        };
    }

    /// Reconstruct registry with custom settings. Registers session_id=2.
    void recreateRegistry(CoordinationSettingsPtr settings,
                          KeeperSession::LocalReadCallback local_read_cb = {})
    {
        auto ctx = std::make_shared<KeeperContext>(true, settings);
        registry = std::make_unique<KeeperSessionRegistry>(
            ctx, test_queue, local_read_cb ? std::move(local_read_cb) : defaultLocalReadCallback());
        auto cb = [this](std::vector<SessionRequestPtr> batch)
        {
            for (auto & r : batch)
                if (r) /// Skip nullptr sentinel from finalizeWithErrors.
                    delivered_requests.push_back(std::move(r));
        };
        registry->registerSession(/*session_id=*/2, std::move(cb));
        session = registry->findSession(2);
    }

    std::vector<LocalRead> local_reads;
    std::vector<SessionRequestPtr> delivered_requests;
    int notify_count = 0;
    int batch_call_count = 0;
    std::vector<size_t> batch_sizes;

    KeeperRequestsQueue test_queue{/*num_subqueues=*/1, /*max_queue_size=*/0};
    std::unique_ptr<KeeperSessionRegistry> registry;
    KeeperSessionPtr session;

    void SetUp() override
    {
        local_reads.clear();
        delivered_requests.clear();
        notify_count = 0;
        batch_call_count = 0;
        batch_sizes.clear();

        auto settings = std::make_shared<CoordinationSettings>();
        auto ctx = std::make_shared<KeeperContext>(true, settings);

        registry = std::make_unique<KeeperSessionRegistry>(
            ctx, test_queue, defaultLocalReadCallback());

        auto callback = [this](std::vector<SessionRequestPtr> batch)
        {
            for (auto & req : batch)
                if (req) /// Skip nullptr sentinel from finalizeWithErrors.
                    delivered_requests.push_back(std::move(req));
        };

        registry->registerSession(/*session_id=*/1, std::move(callback));
        session = registry->findSession(1);
    }

    /// Pull one quorum request from the shared queue (simulates `requestThread`).
    /// Transitions `PendingRaft` → `InRaft` and returns the lightweight Raft struct.
    KeeperRequestForSession submitOne()
    {
        auto batch = test_queue.waitAndPullRaftBatch(/*max_count=*/1, /*max_bytes=*/UINT64_MAX, /*max_batch_time_us=*/0, [] { return false; });
        EXPECT_EQ(batch.entries.size(), 1u);
        if (batch.entries.empty())
            return {};
        return batch.entries[0];
    }

    /// Simulate a successful Raft commit for a write request.
    /// 1. onRaftResponse attaches the response to the FIFO head.
    /// 2. onRaftCommitted calls drainCompleted which pops+delivers+advances.
    void commitSubmitted(const KeeperRequestForSession & submitted)
    {
        auto response = submitted.request->makeResponse();
        response->xid = submitted.request->xid;
        response->zxid = 0;
        response->error = Coordination::Error::ZOK;

        auto result = session->onRaftResponse(submitted.request->xid, response);
        ASSERT_EQ(result, KeeperSession::DeliveryResult::Delivered);

        session->onRaftCommitted();
    }

    /// Simulate a failed Raft commit — finalize session with errors (like terminateSession does).
    void failSession()
    {
        session->finalizeWithErrors(Coordination::Error::ZCONNECTIONLOSS);
    }
};

}

/// Test: write goes to per-session queue, read with no preceding writes goes to fast path.
TEST_F(KeeperSessionTest, WriteGoesToLocalQueue_ReadGoesToFastPath)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(local_reads.size(), 0);

    /// Submit and complete the write so the FIFO is clear.
    auto submitted = submitOne();
    ASSERT_EQ(submitted.request->xid, 1);
    commitSubmitted(submitted);
    ASSERT_EQ(delivered_requests.size(), 1);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);

    /// Read with no preceding writes should take fast path.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));
    ASSERT_EQ(local_reads.size(), 1); // fast path
    ASSERT_EQ(delivered_requests.size(), 2);
    ASSERT_EQ(delivered_requests[1]->request->xid, 2);
}

/// Test: read deferred behind write, released on commit.
TEST_F(KeeperSessionTest, ReadDeferredBehindWrite_ReleasedOnCommit)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));

    /// Read should be PendingLocal, not dispatched yet.
    ASSERT_EQ(local_reads.size(), 0);

    auto submitted = submitOne();
    commitSubmitted(submitted);

    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req->request->xid, 2);
    ASSERT_EQ(delivered_requests.size(), 2);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 2);
}

/// Test: multiple reads deferred behind one write.
TEST_F(KeeperSessionTest, MultipleReadsDeferredBehindOneWrite)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(3))));

    ASSERT_EQ(local_reads.size(), 0);

    auto submitted = submitOne();
    commitSubmitted(submitted);
    /// Reads dispatched as quorum-thread group by onRaftCommitted.
    ASSERT_EQ(local_reads.size(), 2);
    ASSERT_EQ(local_reads[0].req->request->xid, 2);
    ASSERT_EQ(local_reads[1].req->request->xid, 3);
    ASSERT_EQ(delivered_requests.size(), 3);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 2);
    ASSERT_EQ(delivered_requests[2]->request->xid, 3);
}

/// Test: reads deferred behind different writes.
/// With executor model: all writes go directly to InRaft. Reads inherit
/// the quorum executor. onRaftCommitted drains reads, then the next
/// write's commit drains the next read group.
TEST_F(KeeperSessionTest, ReadsDeferredBehindDifferentWrites)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(10))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(20))));

    ASSERT_EQ(local_reads.size(), 0);

    auto w1 = submitOne();
    auto w2 = submitOne();

    /// Commit W1 — drains R10 (quorum-thread group). Stops at W2 (InRaft).
    commitSubmitted(w1);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req->request->xid, 10);

    /// Commit W2 — drains R20.
    commitSubmitted(w2);
    ASSERT_EQ(local_reads.size(), 2);
    ASSERT_EQ(local_reads[1].req->request->xid, 20);
    ASSERT_EQ(delivered_requests.size(), 4);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 10);
    ASSERT_EQ(delivered_requests[2]->request->xid, 2);
    ASSERT_EQ(delivered_requests[3]->request->xid, 20);
}

/// Test: writes always enter Raft immediately (no PendingRaft).
/// The executor boundary prevents the session thread from executing
/// reads behind a write, preserving head-only onRaftResponse.
TEST_F(KeeperSessionTest, WriteAlwaysInRaft_ReadsDrainedByCommit)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(10))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));

    ASSERT_EQ(local_reads.size(), 0);

    auto w1 = submitOne();
    auto w2 = submitOne();
    commitSubmitted(w1);

    /// Committing W1 drains R10 (quorum-thread group). W2 already in Raft.
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req->request->xid, 10);

    ASSERT_EQ(w2.request->xid, 2);
    commitSubmitted(w2);

    ASSERT_EQ(delivered_requests.size(), 3);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 10);
    ASSERT_EQ(delivered_requests[2]->request->xid, 2);
}

/// Test: session failure terminates the session, orphans remaining requests.
TEST_F(KeeperSessionTest, SessionFailure_TerminatesSession)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));

    auto submitted = submitOne();

    /// Terminate session — pending requests are orphaned (no error responses).
    failSession();
    ASSERT_FALSE(session->canAcceptRequests());
}

/// Test: after session failure, no new requests accepted.
TEST_F(KeeperSessionTest, SessionFailure_RejectsNewRequests)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    auto w1 = submitOne();

    failSession();
    ASSERT_NE(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));
    ASSERT_NE(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(3))));
}

/// Test: write response is delivered before deferred reads behind it are released.
TEST_F(KeeperSessionTest, WriteResponsePrecedesDeferredReadResponses)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(10))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(20))));

    auto submitted = submitOne();
    commitSubmitted(submitted);

    ASSERT_EQ(delivered_requests.size(), 3);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 10);
    ASSERT_EQ(delivered_requests[2]->request->xid, 20);
}

/// Test: non-monotonic XIDs (auth xid=-4 after normal write).
TEST_F(KeeperSessionTest, NonMonotonicXIDs)
{
    /// Normal write, then read, then auth (xid=-4).
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(5))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(6))));

    /// Auth request goes through Raft (xid=-4 is non-read).
    auto auth_req = std::make_shared<ZooKeeperAuthRequest>();
    auth_req->xid = -4;
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(auth_req)));

    auto write_req = submitOne();
    auto auth_submitted = submitOne();

    /// Commit write xid=5 — drains R6 (quorum-thread group).
    commitSubmitted(write_req);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req->request->xid, 6);

    /// Commit auth xid=-4 — nothing deferred behind it.
    commitSubmitted(auth_submitted);
    ASSERT_EQ(delivered_requests.size(), 3);
    ASSERT_EQ(delivered_requests[0]->request->xid, 5);
    ASSERT_EQ(delivered_requests[1]->request->xid, 6);
    ASSERT_EQ(delivered_requests[2]->request->xid, -4);
}

/// Test: Close is terminal — no reads can be deferred behind it.
TEST_F(KeeperSessionTest, CloseIsTerminal_RejectsSubsequentRequests)
{
    /// Submit a write, then Close.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    auto close_req = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    close_req->xid = Coordination::CLOSE_XID;
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(close_req)));

    /// Any request after Close is rejected.
    ASSERT_NE(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));
    ASSERT_NE(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(3))));

    /// No reads dispatched, no reads deferred.
    ASSERT_EQ(local_reads.size(), 0);
}

/// Test: Close commit does NOT release deferred reads (there shouldn't be any).
TEST_F(KeeperSessionTest, CloseCommitDoesNotReleaseReads)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    auto close_req = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    close_req->xid = Coordination::CLOSE_XID;
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(close_req)));

    auto submitted_write = submitOne();
    auto submitted_close = submitOne();

    /// Commit the write — no deferred reads behind it since Close follows.
    commitSubmitted(submitted_write);
    ASSERT_EQ(local_reads.size(), 0);

    /// Commit Close — nothing to release.
    commitSubmitted(submitted_close);
    ASSERT_EQ(local_reads.size(), 0);
}

/// Test: Close response is delivered through drainCompleted (not onRaftResponse).
/// Verifies the P1 fix: session must remain findable until onRaftCommitted delivers
/// the Close response, because detachment happens AFTER onRaftCommitted returns.
TEST_F(KeeperSessionTest, CloseDeliveredByDrainCompleted_NotByAttach)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    auto close_req = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    close_req->xid = Coordination::CLOSE_XID;
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(close_req)));

    auto submitted_write = submitOne();
    auto submitted_close = submitOne();

    /// Commit write.
    commitSubmitted(submitted_write);
    ASSERT_EQ(delivered_requests.size(), 1);

    /// onRaftResponse for Close should NOT set detach_session.
    auto close_response = submitted_close.request->makeResponse();
    close_response->xid = submitted_close.request->xid;
    close_response->error = Coordination::Error::ZOK;
    auto attach_result = session->onRaftResponse(submitted_close.request->xid, close_response);
    ASSERT_EQ(attach_result, KeeperSession::DeliveryResult::Delivered);
    ASSERT_NE(attach_result, KeeperSession::DeliveryResult::DeliveredAndDetach); /// Key: no detach from attach

    /// Session is still "findable" (not Closed yet) — the Close response hasn't
    /// been delivered. Only onRaftCommitted -> drainCompleted delivers it.
    session->onRaftCommitted();

    /// Now the Close response should be delivered.
    ASSERT_EQ(delivered_requests.size(), 2);
    ASSERT_EQ(delivered_requests[1]->response->getOpNum(), Coordination::OpNum::Close);

    /// Session should be Closed (no more requests accepted).
    ASSERT_FALSE(session->canAcceptRequests());
}

/// Test: Write behind reads gets New state, then promoted after reads complete.
TEST_F(KeeperSessionTest, WriteBehindReads_PromotedAfterReadsDrain)
{
    /// R1 goes to Active (fast path, queue empty).
    /// After R1 completes, drainCompleted pops R1. Queue is now empty.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(1))));
    ASSERT_EQ(local_reads.size(), 1); // R1 dispatched immediately
    ASSERT_EQ(delivered_requests.size(), 1); // R1 delivered

    /// Now queue is empty again. New write goes directly to RaftProcessing.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));
    auto w2 = submitOne();
    ASSERT_NE(w2.request, nullptr);
    ASSERT_EQ(w2.request->xid, 2);
}

/// Test: Read-Write-Read-Write pattern with interleaved completion.
TEST_F(KeeperSessionTest, ReadWriteReadWrite_InterleavedCompletion)
{
    /// Setup: make local_read NOT fill response so we can control timing.
    bool block_reads = true;
    recreateRegistry(
        std::make_shared<CoordinationSettings>(),
        [this, &block_reads](std::span<SessionRequestPtr> batch)
        {
            for (auto & req : batch)
            {
                local_reads.push_back({req});
                if (!block_reads)
                {
                    auto response = req->request->makeResponse();
                    response->xid = req->request->xid;
                    response->zxid = 0;
                    response->error = Coordination::Error::ZOK;
                    req->response = response;
                }
            }
        });

    /// Unblock reads so they complete synchronously.
    block_reads = false;

    /// R1 -> Active (dispatched, completed, delivered).
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(1))));
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(delivered_requests.size(), 1);

    /// Queue is now empty. W2 -> RaftProcessing.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));

    /// R3 -> PendingLocal (behind W2).
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(3))));
    ASSERT_EQ(local_reads.size(), 1); // not dispatched yet

    /// W4 goes directly to InRaft (executor model: writes always InRaft).
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(4))));

    auto w2 = submitOne();
    auto w4 = submitOne(); // both writes InRaft immediately

    /// Commit W2 -> R3 dispatched (quorum-thread group).
    commitSubmitted(w2);
    ASSERT_EQ(local_reads.size(), 2); // R3 dispatched
    ASSERT_EQ(delivered_requests.size(), 3); // R1, W2, R3

    /// Commit W4 -> nothing behind it.
    commitSubmitted(w4);
    ASSERT_EQ(delivered_requests.size(), 4); // R1, W2, R3, W4
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 2);
    ASSERT_EQ(delivered_requests[2]->request->xid, 3);
    ASSERT_EQ(delivered_requests[3]->request->xid, 4);
}

/// Test: batch read optimization — multiple PendingLocal reads dispatched
/// in a single local_read_batch call after a write commits.
TEST_F(KeeperSessionTest, BatchReadOptimization_SingleBatchCall)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(3))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(4))));

    /// No reads dispatched yet (all deferred behind write).
    ASSERT_EQ(local_reads.size(), 0);
    ASSERT_EQ(batch_call_count, 0);

    /// Commit the write — should trigger a single batch call with 3 reads.
    auto submitted = submitOne();
    commitSubmitted(submitted);

    ASSERT_EQ(batch_call_count, 1);
    ASSERT_EQ(batch_sizes.size(), 1u);
    ASSERT_EQ(batch_sizes[0], 3u);
    ASSERT_EQ(local_reads.size(), 3);

    /// All 4 responses delivered in FIFO order.
    ASSERT_EQ(delivered_requests.size(), 4);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 2);
    ASSERT_EQ(delivered_requests[2]->request->xid, 3);
    ASSERT_EQ(delivered_requests[3]->request->xid, 4);
}

/// Test: single read on empty queue dispatches inline and is delivered.
TEST_F(KeeperSessionTest, SingleRead_InlineDispatch)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(1))));

    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(batch_call_count, 1); // single-element batch
    ASSERT_EQ(batch_sizes[0], 1u);
    ASSERT_EQ(delivered_requests.size(), 1);
}

/// --- quorum_reads = true tests ---

/// Test: with `quorum_reads` enabled, reads go through Raft like writes.
TEST_F(KeeperSessionTest, QuorumReads_ReadsGoThroughRaft)
{
    auto settings = std::make_shared<CoordinationSettings>();
    (*settings)[CoordinationSetting::quorum_reads] = true;
    recreateRegistry(settings);

    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(1))));
    ASSERT_EQ(local_reads.size(), 0);

    auto submitted = submitOne();
    commitSubmitted(submitted);
    ASSERT_EQ(delivered_requests.size(), 1);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
}

/// Test: quorum read behind a write — both go through Raft.
TEST_F(KeeperSessionTest, QuorumReads_ReadBehindWrite)
{
    auto settings = std::make_shared<CoordinationSettings>();
    (*settings)[CoordinationSetting::quorum_reads] = true;
    recreateRegistry(settings);

    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(2))));

    auto w1 = submitOne();
    auto r2 = submitOne();
    commitSubmitted(w1);
    commitSubmitted(r2);
    ASSERT_EQ(delivered_requests.size(), 2);
    ASSERT_EQ(delivered_requests[0]->request->xid, 1);
    ASSERT_EQ(delivered_requests[1]->request->xid, 2);
}

/// --- onWatchNotification tests ---

/// Test: watch notifications bypass the FIFO and are delivered immediately.
TEST_F(KeeperSessionTest, DeliverWatch_BypassesFIFO)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));

    auto watch = std::make_shared<Coordination::ZooKeeperWatchResponse>();
    watch->xid = Coordination::WATCH_XID;
    watch->path = "/test";
    watch->type = Coordination::Event::CHANGED;
    watch->state = Coordination::State::CONNECTED;

    auto result = session->onWatchNotification(watch);
    ASSERT_EQ(result, KeeperSession::DeliveryResult::Delivered);
    ASSERT_NE(result, KeeperSession::DeliveryResult::DeliveredAndDetach);
    ASSERT_EQ(delivered_requests.size(), 1);
    ASSERT_TRUE(delivered_requests[0]->is_watch_notification);
}

/// --- deliverDirect tests ---

/// Test: `deliverDirect` delivers directly, bypassing the FIFO.
TEST_F(KeeperSessionTest, DeliverResponse_DirectDelivery)
{
    auto response = std::make_shared<Coordination::ZooKeeperGetResponse>();
    response->xid = 42;
    response->error = Coordination::Error::ZSESSIONEXPIRED;

    auto result = session->deliverDirect(response);
    ASSERT_EQ(result, KeeperSession::DeliveryResult::Delivered);
    ASSERT_NE(result, KeeperSession::DeliveryResult::DeliveredAndDetach);
    ASSERT_EQ(delivered_requests.size(), 1);
    ASSERT_EQ(delivered_requests[0]->response->error, Coordination::Error::ZSESSIONEXPIRED);
}

/// Test: `deliverDirect` returns NotDelivered on Closed session.
TEST_F(KeeperSessionTest, DeliverResponse_ClosedSessionRejects)
{
    session->finalizeWithErrors(Coordination::Error::ZSESSIONEXPIRED);

    auto response = std::make_shared<Coordination::ZooKeeperGetResponse>();
    response->xid = 1;
    response->error = Coordination::Error::ZOK;

    auto result = session->deliverDirect(response);
    ASSERT_EQ(result, KeeperSession::DeliveryResult::NotDelivered);
}

/// --- finalizeWithErrors tests ---

/// Test: `finalizeWithErrors` delivers error responses for all active requests in FIFO order.
TEST_F(KeeperSessionTest, FinalizeWithErrors_DeliversErrorsForAllActiveRequests)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeReadRequest(3))));

    auto result = session->finalizeWithErrors(Coordination::Error::ZCONNECTIONLOSS);
    ASSERT_TRUE(result);

    /// All three requests should be delivered with error responses.
    ASSERT_EQ(delivered_requests.size(), 3u);
    for (size_t i = 0; i < delivered_requests.size(); ++i)
    {
        ASSERT_TRUE(delivered_requests[i]->response);
        ASSERT_EQ(delivered_requests[i]->response->error, Coordination::Error::ZCONNECTIONLOSS);
    }

    /// Verify FIFO order by xid.
    ASSERT_EQ(delivered_requests[0]->response->xid, 1);
    ASSERT_EQ(delivered_requests[1]->response->xid, 2);
    ASSERT_EQ(delivered_requests[2]->response->xid, 3);

    /// Session is Closed.
    ASSERT_FALSE(session->canAcceptRequests());
}

/// Test: `finalizeWithErrors` preserves already-attached Raft responses.
TEST_F(KeeperSessionTest, FinalizeWithErrors_PreservesAlreadyAttachedResponses)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(2))));

    /// Submit first write to Raft and simulate successful commit response
    /// (attach response to FIFO head, but don't drain).
    auto submitted = submitOne();
    auto ok_response = submitted.request->makeResponse();
    ok_response->xid = submitted.request->xid;
    ok_response->zxid = 42;
    ok_response->error = Coordination::Error::ZOK;
    session->onRaftResponse(submitted.request->xid, ok_response);

    /// Now finalize with errors — first entry should keep its committed response.
    session->finalizeWithErrors(Coordination::Error::ZSESSIONEXPIRED);

    ASSERT_EQ(delivered_requests.size(), 2u);

    /// First request: already had a response (committed), should keep it.
    ASSERT_EQ(delivered_requests[0]->response->error, Coordination::Error::ZOK);
    ASSERT_EQ(delivered_requests[0]->response->xid, 1);

    /// Second request: no response yet, gets error.
    ASSERT_EQ(delivered_requests[1]->response->error, Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_EQ(delivered_requests[1]->response->xid, 2);
}

/// Test: `finalizeWithErrors` on empty queue returns false, no deliveries.
TEST_F(KeeperSessionTest, FinalizeWithErrors_EmptyQueue_ReturnsFalse)
{
    auto result = session->finalizeWithErrors(Coordination::Error::ZCONNECTIONLOSS);
    ASSERT_FALSE(result);
    ASSERT_TRUE(delivered_requests.empty());
}

/// Test: `finalizeWithErrors` is idempotent — second call returns false.
TEST_F(KeeperSessionTest, FinalizeWithErrors_Idempotent)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));

    auto first = session->finalizeWithErrors(Coordination::Error::ZCONNECTIONLOSS);
    ASSERT_TRUE(first);
    ASSERT_EQ(delivered_requests.size(), 1u);

    auto second = session->finalizeWithErrors(Coordination::Error::ZCONNECTIONLOSS);
    ASSERT_FALSE(second);
    /// No additional deliveries.
    ASSERT_EQ(delivered_requests.size(), 1u);
}

/// Test: `finalizeWithErrors` on already-Closed session returns false.
TEST_F(KeeperSessionTest, FinalizeWithErrors_ClosedSession_ReturnsFalse)
{
    session->finalizeWithErrors(Coordination::Error::ZSESSIONEXPIRED);

    auto result = session->finalizeWithErrors(Coordination::Error::ZCONNECTIONLOSS);
    ASSERT_FALSE(result);
    ASSERT_TRUE(delivered_requests.empty());
}

/// Test: `addRequest` returns false after `finalizeWithErrors`.
TEST_F(KeeperSessionTest, FinalizeWithErrors_RejectsNewRequests)
{
    session->finalizeWithErrors(Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_NE(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
}

/// Test: `addRequest` rejects when per-session active request limit is reached.
TEST(KeeperSessionLimitTest, AddRequest_RejectsWhenActiveRequestLimitReached)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::max_session_active_requests] = 2;
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);

    KeeperRequestsQueue queue{1, 0};
    KeeperSessionRegistry registry(keeper_context, queue, [](std::span<SessionRequestPtr>) {});

    std::vector<SessionRequestPtr> delivered;
    auto callback = [&](std::vector<SessionRequestPtr> batch)
    {
        for (auto & req : batch)
            delivered.push_back(std::move(req));
    };

    registry.registerSession(1, std::move(callback));
    auto session = registry.findSession(1);

    auto makeReq = [](XID xid)
    {
        using namespace std::chrono;
        auto zk_req = std::make_shared<ZooKeeperCreateRequest>();
        zk_req->xid = xid;
        zk_req->path = "/test";
        auto req = std::make_shared<SessionRequest>();
        req->session_id = 1;
        req->cached_bytes_size = zk_req->bytesSize();
        req->request = std::move(zk_req);
        req->time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        return req;
    };

    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeReq(1)));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeReq(2)));
    /// Third request exceeds limit of 2.
    ASSERT_NE(KeeperSession::AddResult::Accepted, session->addRequest(makeReq(3)));
}

/// --- Close bypass tests ---

/// Test: Close bypasses per-session active request limit.
TEST(KeeperSessionLimitTest, CloseBypasses_PerSessionLimit)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::max_session_active_requests] = 2;
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);

    KeeperRequestsQueue queue{1, 0};
    KeeperSessionRegistry registry(keeper_context, queue, [](std::span<SessionRequestPtr>) {});

    registry.registerSession(1, [](std::vector<SessionRequestPtr>) {});
    auto session = registry.findSession(1);

    auto makeReq = [](XID xid, Coordination::OpNum op = Coordination::OpNum::Create)
    {
        using namespace std::chrono;
        Coordination::ZooKeeperRequestPtr zk_req;
        if (op == Coordination::OpNum::Close)
        {
            zk_req = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
            zk_req->xid = xid;
        }
        else
        {
            auto create = std::make_shared<ZooKeeperCreateRequest>();
            create->xid = xid;
            create->path = "/test";
            zk_req = create;
        }
        auto req = std::make_shared<SessionRequest>();
        req->session_id = 1;
        req->cached_bytes_size = zk_req->bytesSize();
        req->request = std::move(zk_req);
        req->time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        return req;
    };

    /// Fill to limit.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeReq(1)));
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeReq(2)));

    /// Normal request rejected.
    ASSERT_EQ(KeeperSession::AddResult::QueueFull, session->addRequest(makeReq(3)));

    /// Close still accepted despite limit.
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeReq(Coordination::CLOSE_XID, Coordination::OpNum::Close)));
}

/// --- onRaftResponse edge cases ---

/// Test: onRaftResponse on Closed session returns NotDelivered.
TEST_F(KeeperSessionTest, OnRaftResponse_ClosedSession_ReturnsNotDelivered)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    auto submitted = submitOne();

    session->finalizeWithErrors(Coordination::Error::ZSESSIONEXPIRED);

    auto response = submitted.request->makeResponse();
    response->xid = 1;
    response->error = Coordination::Error::ZOK;
    auto result = session->onRaftResponse(1, response);
    ASSERT_EQ(result, KeeperSession::DeliveryResult::NotDelivered);
}

/// Test: onRaftResponse on empty FIFO returns NotDelivered.
TEST_F(KeeperSessionTest, OnRaftResponse_EmptyFifo_ReturnsNotDelivered)
{
    auto response = std::make_shared<Coordination::ZooKeeperGetResponse>();
    response->xid = 1;
    response->error = Coordination::Error::ZOK;
    auto result = session->onRaftResponse(1, response);
    ASSERT_EQ(result, KeeperSession::DeliveryResult::NotDelivered);
}

/// Test: onRaftResponse with wrong XID returns FifoMismatch.
TEST_F(KeeperSessionTest, OnRaftResponse_XidMismatch_ReturnsFifoMismatch)
{
    ASSERT_EQ(KeeperSession::AddResult::Accepted, session->addRequest(makeKeeperReq(makeWriteRequest(1))));
    auto submitted = submitOne();

    auto response = submitted.request->makeResponse();
    response->xid = 99; /// Wrong XID.
    response->error = Coordination::Error::ZOK;
    auto result = session->onRaftResponse(99, response);
    ASSERT_EQ(result, KeeperSession::DeliveryResult::FifoMismatch);
}

/// --- KeeperSessionRegistry lifecycle ---

/// Test: register, find, detach, find-after-detach, detach-idempotent.
TEST(KeeperSessionRegistryTest, RegisterFindDetach)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    KeeperRequestsQueue queue{1, 0};
    KeeperSessionRegistry registry(ctx, queue, [](std::span<SessionRequestPtr>) {});

    registry.registerSession(42, [](std::vector<SessionRequestPtr>) {});

    ASSERT_NE(registry.findSession(42), nullptr);
    ASSERT_EQ(registry.findSession(99), nullptr); /// Non-existent.

    auto detached = registry.detachSession(42);
    ASSERT_NE(detached, nullptr);
    ASSERT_EQ(registry.findSession(42), nullptr); /// Gone.

    /// Idempotent — second detach returns nullptr.
    ASSERT_EQ(registry.detachSession(42), nullptr);
}

/// Test: shutdown returns all sessions and clears the registry.
TEST(KeeperSessionRegistryTest, ShutdownReturnsAllSessions)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    KeeperRequestsQueue queue{1, 0};
    KeeperSessionRegistry registry(ctx, queue, [](std::span<SessionRequestPtr>) {});

    registry.registerSession(1, [](std::vector<SessionRequestPtr>) {});
    registry.registerSession(2, [](std::vector<SessionRequestPtr>) {});
    registry.registerSession(3, [](std::vector<SessionRequestPtr>) {});

    auto sessions = registry.shutdown();
    ASSERT_EQ(sessions.size(), 3u);

    /// All sessions gone from registry.
    ASSERT_EQ(registry.findSession(1), nullptr);
    ASSERT_EQ(registry.findSession(2), nullptr);
    ASSERT_EQ(registry.findSession(3), nullptr);
}

/// Test: SessionRequest default state is Initial.
TEST(SessionRequestTest, DefaultStateIsInitial)
{
    auto req = makeWriteRequest(1);
    auto env = std::make_shared<SessionRequest>();
    env->session_id = 1;
    env->request = req;

    ASSERT_EQ(env->getState(), RequestState::Initial);
}

/// Test: SessionRequest destructor doesn't exception on never-initialized spans.
/// This verifies the safety net — destroying a request without calling
/// any lifecycle method must not trigger an assertion on start_time_us == 0.
TEST(SessionRequestTest, DestructorSafeOnNeverInitializedSpans)
{
    auto req = makeReadRequest(1);
    {
        auto env = std::make_shared<SessionRequest>();
        env->session_id = 1;
        env->request = req;
        /// No lifecycle method called — spans are never initialized.
        /// Destructor must not abort.
    }
    /// Test passes by not crashing — no assertion needed.
}

#endif
