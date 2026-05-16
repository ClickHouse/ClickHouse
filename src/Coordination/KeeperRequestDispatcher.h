#pragma once

#include "config.h"

#if USE_NURAFT

#include <Common/CacheLine.h>
#include <Common/NonblockingBoundedQueue.h>
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
///    between also preserving order. See nuraft::client_req_stream.
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
/// KeeperRequestDispatcher is in charge of most of flow control in keeper server.
/// I.e. preventing queue size bloat in the whole keeper server + nuraft pipeline. Especially if
/// some part of the pipeline is not keeping up; e.g. clients are sending more requests than nuraft
/// can process, or keeper server is trying to send more replies than clients are ready to receive.
/// Here's a story for why no queues or buffers throughout keeper can grow out of control:
///  * Requests from clients arrive in server's TCP sockets (memory usage limited by the OS) and go
///    through some ReadBuffer-s (size limited per client connection), and are parsed by KeeperTCPHandler.
///  * Request is then passed to KeeperRequestDispatcher, which puts it in requests_queue, which
///    has a limited size (in bytes).
///  * KeeperRequestDispatcher then puts requests into batches and sends them on a journey through
///    the whole raft pipeline. It also keeps track of the in-flight requests and makes sure there
///    aren't too many; this is sufficient to ensure no queues or buffers explode anywhere in
///    the whole raft pipeline. So we don't have to carefully choose queue sizes etc everywhere
///    throughout nuraft.
///    (All intermediate messages inside nuraft are small, so there's no situation where our
///     moderate amount of requests translates to disproportionately large memory usage inside nuraft.)
///  * After going through nuraft, newly committed requests end up in KeeperStateMachine<Storage>::commit,
///    which produces responses and passes them to KeeperRequestDispatcher::onResponse.
///    From onResponse they go to responses_queue, then to KeeperTCPHandler::responses (through responseThread).
///    The total size of responses_queue + all KeeperTCPHandler::responses queues is tracked by
///    KeeperRequestDispatcher::response_bytes_in_all_queues.
///    If that total size gets too big, onResponse just sleeps, delaying the commit thread
///    (or any other thread that produces responses) and preventing allocation of further responses
///    until queues gets smaller.
///  * Finally, KeeperTCPHandler takes responses from its queue and passes them to WriteBuffer,
///    which passes them to the socket. Again the WriteBuffer memory is limited
///    (per client connection), and socket memory is limited by the OS. And we're done!
///
/// ---
///
/// (KeeperRequestDispatcher implementation goes all fancy on avoiding locks and grouping atomics.
///  This is mostly just for fun and for practice; a much sloppier implementation would probably be
///  equally fast because KeeperRequestDispatcher shouldn't be the bottleneck.
///  One part where performance may matter is commit callback; we shouldn't waste any time there
///  because the commit thread is likely a bottleneck.)
class KeeperRequestDispatcher
{
public:
    explicit KeeperRequestDispatcher(KeeperServer * server_);

    void startup();

    /// closed_all_connections is used just for an assert: if true, we expect that all
    /// onResponseDeallocated calls were made, so the tracked response queue size should be zero.
    void shutdown(bool closed_all_connections);

    /// May block for up to operation_timeout_ms if queue is full.
    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64);

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    void finishSession(int64_t session_id);

    /// May block for a short time if queue is full.
    void onResponse(KeeperResponseForSession response) noexcept;

    void onCommit(const KeeperRequestForSession & request_for_session);

    /// KeeperRequestDispatcher is in charge of preventing response queue(s) from growing very big and OOMing.
    /// This covers KeeperRequestDispatcher's own responses_queue, and `responses` queues from all KeeperTCPHandler-s.
    /// (After that the response goes to WriteBuffer and the OS TCP stack, which both have their own size limits.)
    /// Must be called eventually for every ZooKeeperResponseCallback call.
    void onResponseDeallocated(const Coordination::ZooKeeperResponse & response);

private:
    /// Suppose we get a write request from some session and put it in batch B and send that
    /// batch to leader. While B is still in flight, we get a read request from the same session.
    /// We'd like to execute that read as soon as B is committed. So we want a list of such
    /// late-added reads inside InFlightBatch, with careful synchronization between dispatchThread
    /// adding to the list and onCommit taking from it. This is that list.
    class alignas(CH_CACHE_LINE_SIZE) LateReads
    {
    public:
        /// Returns false if Status::Finished, i.e. the calling dispatchThread can go ahead and
        /// do the read right there.
        bool add(KeeperRequestForSession & request_for_session)
        {
            if (!lock())
                return false;
            reads.push_back(std::move(request_for_session));
            unlock(Status::Available);
            return true;
        }

        /// Moves reads out of the container.
        /// If it's empty, changes status to Finished, otherwise back to Available.
        /// (See comment in onCommit for explanation.)
        KeeperRequestsForSessions takeAndFinishIfEmpty()
        {
            if (!lock())
            {
                chassert(false);
                return {};
            }
            KeeperRequestsForSessions res = std::move(reads);
            Status to = res.empty() ? Status::Finished : Status::Available;
            unlock(to);
            return res;
        }

        /// Set to Finished, drop reads.
        void deactivate()
        {
            if (lock())
            {
                reads.clear();
                unlock(Status::Finished);
            }
        }

        /// Set to Available.
        void activate(KeeperRequestsForSessions initial_late_reads)
        {
            chassert(reads.empty());
            reads = std::move(initial_late_reads);
            Status s = status.exchange(Status::Available);
            chassert(s == Status::Finished);
        }

    private:
        /// A spinlock and a flag saying whether new reads can be added.
        enum Status
        {
            /// Can add more reads.
            Available,
            /// `reads` is being mutated, either by dispatchThread or by onCommit.
            Busy,
            /// Batch committed, reads finished.
            Finished,
        };

        std::atomic<Status> status {Status::Finished};
        KeeperRequestsForSessions reads;

        bool lock()
        {
            /// Available -> Busy
            /// Busy -> wait
            /// Finished -> return false
            Status s = Status::Available;
            while (!status.compare_exchange_weak(s, Status::Busy))
            {
                if (s == Status::Finished)
                    return false;
                s = Status::Available;
            }
            return true;
        }

        void unlock(Status to)
        {
            Status s = status.exchange(to);
            chassert(s == Status::Busy);
        }

        void setStatus(Status target)
        {
            chassert(target != Status::Busy);
            Status s = status.load();
            while (true)
            {
                if (s == Status::Busy)
                    s = status.load();
                else if (status.compare_exchange_weak(s, target))
                    break;
            }
        }
    };

    struct InFlightBatch
    {
        /// If false, this is a vacant slot in the in_flight_batches array, not between head_idx and tail_idx.
        std::atomic<bool> active {};

        alignas(CH_CACHE_LINE_SIZE) std::chrono::steady_clock::time_point start_time {};
        KeeperRequestsForSessions requests;
        size_t committed_requests = 0;
        /// Read requests to do after some prefix of `requests` is committed.
        /// Element <next_request_idx, read_requests> means that these read_requests must be
        /// executed just after request requests[next_request_idx - 1].
        /// 0 < next_request_idx < requests.size().
        /// Reads to do after the last request go into late_reads instead.
        std::vector<std::pair</*next_request_idx*/ size_t, KeeperRequestsForSessions>> intermediate_reads;
        /// Index in intermediate_reads up to which we've processed the reads.
        /// (We could make intermediate_reads an std::queue instead, but I'm a little paranoid about std::deque memory overhead.)
        size_t intermediate_reads_idx = 0;

        LateReads late_reads;

        void activate(KeeperRequestsForSessions initial_late_reads)
        {
            committed_requests = 0;
            intermediate_reads_idx = 0;
            late_reads.activate(std::move(initial_late_reads));
            active.store(true);
        }

        void deactivate()
        {
            requests.clear();
            intermediate_reads.clear();
            late_reads.deactivate();
            active.store(false);
        }
    };

    struct Session
    {
        ZooKeeperResponseCallback response_callback;
        std::atomic<bool> dead {};

        /// Latest InFlightBatch that has read or write requests from this session.
        size_t last_batch_idx = 0;

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
    /// One producer: dispatchThread.
    /// Two consumers, synchronized through request_completion_mutex.
    /// head_idx is incremented after the slot is fully vacated.
    std::vector<InFlightBatch> in_flight_batches;

    nuraft::ptr<nuraft::client_req_stream> stream;

    /// True if we should sleep for a bit before trying to create a new stream, to avoid spamming
    /// retries when the leader not ready to process requests.
    /// Set to false when any request succeeds.
    std::atomic<bool> current_stream_is_suspect {false};

    NonblockingBoundedQueue<KeeperRequestForSession> requests_queue;
    std::atomic<int64_t> requests_queue_bytes {};

    /// TODO: Currently responses to all requests are formed on all nodes, and go through this queue
    ///       and through responseThread and session id lookup on all nodes, and all except one node
    ///       discard the response. Consider adding server id to the request so that we can tell
    ///       early that response is not needed. It would also save time in
    ///       KeeperRequestDispatcher::onCommit, we won't have to check most requests against the queue.
    /// TODO: Maybe we should remove this and pass responses directly to KeeperTCPHandler queues.
    ///       But this intermediate queue may in theory improve performance: it's much cheaper to
    ///       push a batch of responses to this queue than to push individual responses to the
    ///       corresponding sessions' different queues and notify their condition_variable-s
    ///       (FUTEX_WAKE syscall); and that pushing happens from the commit thread, which is often
    ///       on the critical path limiting the total server throughput, while responseThread is
    ///       ~never the bottleneck.
    NonblockingBoundedQueue<KeeperResponseForSession> responses_queue;
    std::atomic<int64_t> response_bytes_in_all_queues {};

    /// State frequently mutated by dispatch thread.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> tail_idx {1};

    /// State frequently mutated by commit thread.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> head_idx {1};
    /// Locked in commit callback and in dropInFlightRequests.
    /// Normally there's no contention because commit callback is called from one thread, and
    /// dropInFlightRequests is very rare.
    std::mutex request_completion_mutex;

    void dispatchThread();
    void responseThread();

    void executeReads(KeeperRequestsForSessions reads);

    void popBatch(size_t batch_idx);
    bool tryPopRequest(KeeperRequestForSession & request); // call instead of requests_queue.tryPop

    void recreateStreamWithBackoff();
    void dropInFlightRequests();

    void addErrorResponse(const KeeperRequestForSession & request_for_session, Coordination::Error error);
};

}

#endif
