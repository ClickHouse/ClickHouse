#pragma once

#include <Core/Block.h>
#include <Common/TraceSender.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

struct ProfileTracesRegistry;
class InternalProfileTracesQueue;
using InternalProfileTracesQueuePtr = std::shared_ptr<InternalProfileTracesQueue>;

/// A query's bounded subscription to the sampling collector. Only the numeric subscription ID
/// crosses the signal-safe pipe; neither query IDs nor object addresses identify subscriptions.
class InternalProfileTracesQueue
{
public:
    static InternalProfileTracesQueuePtr create(const String & query_id);
    ~InternalProfileTracesQueue();

    UInt64 getId() const { return id; }
    static Block getSampleBlock();
    Block getBlock();
    void pushBlock(const Block & block);

    /// Wait for samples already written to the collector pipe, then close this subscription.
    /// On success the queued samples remain available to the final transport drain.
    /// On timeout they are discarded and a warning is logged without failing the query.
    void finish();

    /// Disable delivery immediately, discarding queued samples without waiting for the collector.
    void cancel();

private:
    friend class TraceCollector;

    struct Sample
    {
        String host_name;
        String query_id;
        String trace_type;
        UInt64 thread_id = 0;
        UInt64 event_time_microseconds = 0;
        std::vector<UInt64> trace;
        Strings symbols;
        Int64 size = 0;
        bool symbolized = false;

        size_t byteSize() const;
    };

    InternalProfileTracesQueue(std::shared_ptr<ProfileTracesRegistry> registry_, UInt64 id_, const String & query_id_);
    void push(Sample sample);

    static std::shared_ptr<ProfileTracesRegistry> getRegistry();
    static void collect(
        const std::shared_ptr<ProfileTracesRegistry> & registry, UInt64 id, TraceType trace_type,
        UInt64 thread_id, UInt64 event_time_microseconds, const std::vector<UInt64> & trace, Int64 size);
    static void acknowledgeFlush(const std::shared_ptr<ProfileTracesRegistry> & registry, UInt64 id);

    const std::shared_ptr<ProfileTracesRegistry> registry;
    const UInt64 id;
    const String query_id;
    const String host_name;

    std::mutex mutex;
    std::condition_variable flushed;
    std::deque<Sample> samples;
    size_t buffered_bytes = 0;
    /// Metadata is kept outside the bounded sample queue so overflow cannot hide its own loss count.
    Int64 pending_dropped = 0;
    bool pending_incomplete = false;
    bool flush_acknowledged = false;
    bool finished = false;
};

}
