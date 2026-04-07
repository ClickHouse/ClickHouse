#pragma once

#include "config.h"

#if USE_NURAFT

#include <Common/CacheLine.h>
#include <Common/NonblockingBoundedQueue.h>
#include <Coordination/KeeperAppendStream.h>
#include <Coordination/KeeperServer.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

extern template class NonblockingBoundedQueue<DB::KeeperRequestForSession>;
extern template class NonblockingBoundedQueue<DB::KeeperResponseForSession>;

namespace DB
{

/// Takes client requests from KeeperTCPHandler (or whoever else calls putRequest), passes them
/// to nuraft leader, detects when they're committed (or failed), and passes responses back to
/// KeeperTCPHandler (or whoever registers their response callback by calling registerSession).
///
/// Considerations:
///  * Forwarding. If nuraft leader is not on this node, requests are sent to the leader node.
///  * Batching. responseThread() picks as many consecutive requests as possible to send together,
///    to save the overhead of passing tiny individual requests around.
///  * Pipelining. Multiple request batches can be in flight simultaneously, to keep the whole
///    nuraft pipeline busy, for full resource utilization for maximum throughput.
///    E.g. maybe one batch is being processed by followers, while the next batch is being
///    transferred from leader to followers, while the next batch is being preprocessed by leader.
///  * Ordering. Requests from one client session must be executed in the order they arrived.
///    To preserve order even when multiple requests are executed concurrently, we rely on nuraft
///    leader not reordering requests, on tcp not reordering messages, and on all the plumbing in
///    between also preserving order. See KeeperAppendStream.
///    If leader changes or we lose connection to it, we fail all in-flight requests (their outcome
///    is unknown) and open a new stream.
///  * Executing reads locally. Read requests don't need to go through raft, and don't even have to
///    be forwarded to the leader node. But they must be executed at exactly the right time: after
///    all previous write requests in the same client session were applied, before the next
///    request in the session is applied.
///  * Reordering requests. Requests in the same client session can't be reordered, but we're free
///    to interleave requests from different sessions however we like. We use this freedom to group
///    read requests together as much as possible. When we have many consecutive read requests, we
///    can execute them in parallel (while blocking writes, so that znode state doesn't change).
///  * Minimizing load on the commit thread. Commit thread is usually the bottleneck for throughput
///    of the whole keeper cluster. We should do as little as possible in the commit callback.
///  * Flow control. See below.
///
/// KeeperRequestDispatcher2 is in charge of most of flow control in keeper server.
/// I.e. preventing queue size bloat in the whole keeper server + nuraft pipeline. Especially if
/// some part of the pipeline is not keeping up; e.g. clients are sending more requests than nuraft
/// can process, or keeper server is trying to send more replies than clients are ready to receive.
/// Here's a story for why no queues or buffers throughout keeper can grow out of control:
///  * Requests from clients arrive in server's TCP sockets (memory usage limited by the OS) and go
///    through some ReadBuffer-s (size limited per client connection), and are parsed by KeeperTCPHandler.
///  * Request is then passed to KeeperRequestDispatcher2, which puts it in requests_queue, which
///    has a limited size (in bytes).
///  * KeeperRequestDispatcher2 then puts requests into batches and sends them on a journey through
///    the whole raft pipeline. It also keeps track of the in-flight requests and makes sure there
///    aren't too many; this is sufficient to ensure no queues or buffers explode anywhere in
///    the whole raft pipeline. So we don't have to carefully choose queue sizes etc everywhere
///    throughout nuraft.
///    (All intermediate messages inside nuraft are small, so there's no situation where our
///     moderate amount of requests translates to disproportionately large memory usage inside nuraft.)
///  * After going through nuraft, newly committed requests end up in KeeperStateMachine<Storage>::commit,
///    which produces responses and passes them to KeeperRequestDispatcher2::onResponse.
///    From onResponse they go to responses_queue, then to KeeperTCPHandler::responses (through responseThread).
///    The total size of responses_queue + all KeeperTCPHandler::responses queues is tracked by
///    KeeperRequestDispatcher2::response_bytes_in_all_queues.
///    If that total size gets too big, onResponse just sleeps, delaying the commit thread
///    (or any other thread that produces responses) and preventing allocation of further responses
///    until queues gets smaller.
///  * Finally, KeeperTCPHandler takes responses from its queue and passes them to WriteBuffer,
///    which passes them to the socket. Again the WriteBuffer memory is limited
///    (per client connection), and socket memory is limited by the OS. And we're done!
///
/// ---
///
/// (KeeperRequestDispatcher2 implementation goes all fancy on avoiding locks and grouping atomics.
///  This is mostly just for fun and for practice; a much sloppier implementation would probably be
///  equally fast because KeeperRequestDispatcher2 shouldn't be the bottleneck.
///  One part where performance matters is commit callback; we shouldn't waste any time there
///  because the commit thread is likely a bottleneck.)
class KeeperRequestDispatcher2
{
public:
    explicit KeeperRequestDispatcher2(KeeperServer * server_);

    /// closed_all_connections is used just for an assert: if true, we expect that all
    /// onResponseDeallocated calls were made, so the tracked response queue size should be zero.
    void shutdown(bool closed_all_connections);

    /// May block for up to operation_timeout_ms if queue is full.
    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64);
    bool putLocalReadRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    void finishSession(int64_t session_id);

    /// May block for a short time if queue is full.
    void onResponse(KeeperResponseForSession response) noexcept;

    void onCommit(const KeeperRequestForSession & request_for_session);

    /// KeeperRequestDispatcher2 is in charge of preventing response queue(s) from growing very big and OOMing.
    /// This covers KeeperRequestDispatcher2's own responses_queue, and `responses` queues from all KeeperTCPHandler-s.
    /// (After that the response goes to WriteBuffer and the OS TCP stack, which both have their own size limits.)
    /// Must be called eventually for every ZooKeeperResponseCallback call.
    void onResponseDeallocated(const Coordination::ZooKeeperResponse & response);

private:
    struct InFlightBatch
    {
        /// If false, this is a vacant slot in the in_flight_batches array, not between head_idx and tail_idx.
        std::atomic<bool> active {};

        alignas(CH_CACHE_LINE_SIZE) size_t bytes = 0;
        std::chrono::steady_clock::time_point start_time;
        KeeperRequestsForSessions requests;
        size_t committed_requests = 0;
        /// Element <next_request_idx, read_requests> means that these read_request must be executed
        /// just after request requests[next_request_idx - 1].
        std::vector<std::pair</*next_request_idx*/ size_t, KeeperRequestsForSessions>> reads;
        size_t reads_idx = 0;

        void activate()
        {
            committed_requests = 0;
            reads_idx = 0;
            active.store(true);
        }

        void deactivate()
        {
            requests.clear();
            reads.clear();
            active.store(false);
        }
    };

    struct Session
    {
        ZooKeeperResponseCallback response_callback;
        std::atomic<bool> dead {};

        /// A flag used by request batching to detect dependencies between reads and writes.
        size_t reordering_version = 0;
    };

    KeeperServer * server;
    KeeperContextPtr keeper_context;
    LoggerPtr log;

    ThreadFromGlobalPool dispatch_thread;
    ThreadFromGlobalPool response_thread;

    /// Locked exclusively when adding or removing `sessions` entries.
    SharedMutex sessions_mutex;
    std::unordered_map<int64_t, Session> sessions;

    size_t current_reordering_version = 1;

    std::atomic<bool> shutting_down {};

    /// Lock-free SPSC queue. "Tail" is the enqueue side, "head" is the dequeue side.
    /// Queue size is tail_idx - head_idx.
    /// Queue byte size (sum of InFlightBatch::bytes for batches in the queue) is tail_bytes - head_bytes.
    /// One producer: dispatchThread.
    /// Two consumers, synchronized through request_completion_mutex.
    /// head_idx is incremented after the slot is fully vacated.
    std::vector<InFlightBatch> in_flight_batches;

    std::shared_ptr<KeeperAppendStream> stream;

    /// True if no requests sent through the current `stream` succeeded yet.
    std::atomic<bool> current_stream_is_suspect {};

    NonblockingBoundedQueue<KeeperRequestForSession> requests_queue;
    std::atomic<int64_t> requests_queue_bytes {};

    /// TODO: Currently responses to all requests are formed on all nodes, and go through this queue
    ///       and through responseThread and session id lookup on all nodes, and all except one node
    ///       discard the response. Consider adding server id to the request so that we can tell
    ///       early that response is not needed. It would also save time in
    ///       KeeperRequestDispatcher2::onCommit, we won't have to check most requests against the queue.
    NonblockingBoundedQueue<KeeperResponseForSession> responses_queue;
    std::atomic<int64_t> response_bytes_in_all_queues {};

    /// State frequently mutated by dispatch thread.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> tail_idx {};

    /// State frequently mutated by commit thread.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> head_idx {};
    /// Locked in commit callback and in dropInFlightRequests.
    /// Normally there's no contention because commit callback is called from one thread, and
    /// dropInFlightRequests is very rare.
    std::mutex request_completion_mutex;

    void dispatchThread();
    void responseThread();

    void popBatch(size_t batch_idx);
    bool tryPopRequest(KeeperRequestForSession & request); // call instead of requests_queue.tryPop

    void dropInFlightRequests();

    void addErrorResponse(const KeeperRequestForSession & request_for_session, Coordination::Error error);
};

}

#endif
