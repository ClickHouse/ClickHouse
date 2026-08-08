#pragma once

#include <base/strong_typedef.h>
#include <base/types.h>

#include <atomic>
#include <memory>

#include <Common/MapWithMemoryTracking.h>
#include <Common/NonblockingBoundedQueue.h>
#include <Common/VectorWithMemoryTracking.h>

#include <boost/noncopyable.hpp>

#include <Poco/Channel.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>

namespace ProfileEvents
{
using Event = StrongTypedef<size_t, struct EventTag>;
}

namespace DB
{

class OwnFormattingChannel;
class InternalTextLogsQueue;
template <typename>
class SystemLogQueue;
struct TextLogElement;
using TextLogQueue = SystemLogQueue<TextLogElement>;

using AsyncLogQueueSize = std::pair<std::string, size_t>;
using AsyncLogQueueSizes = VectorWithMemoryTracking<AsyncLogQueueSize>;

class ExtendedLogMessage;
enum class ThreadName : uint8_t;

class OwnSplitChannelBase : public Poco::Channel
{
public:
    using ChannelPtr = std::shared_ptr<OwnFormattingChannel>;

    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const Poco::Message & msg) override = 0;
    void log(Poco::Message && msg) override = 0;

    virtual void setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value) = 0;

    /// Adds a child channel
    virtual void addChannel(
        ChannelPtr channel,
        const std::string & name,
        int level,
        const ProfileEvents::Event & event_on_passed_message_,
        const ProfileEvents::Event & event_on_dropped_message_)
        = 0;

    virtual void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority) = 0;

    virtual void setLevel(const std::string & name, int level) = 0;
};

/// Works as Poco::SplitterChannel, but performs additional work:
///  passes logs to Client via TCP interface
///  tries to use extended logging interface of child for more comprehensive logging
class OwnSplitChannel final : public OwnSplitChannelBase
{
public:
    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const Poco::Message & msg) override;
    void log(Poco::Message && msg) override;

    void open() override;
    void close() override;

    void setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value) override;

    /// Adds a child channel
    void addChannel(
        ChannelPtr channel, const std::string & name, int level, const ProfileEvents::Event &, const ProfileEvents::Event &) override;

    void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority) override;

    void setLevel(const std::string & name, int level) override;

    void logSplit(
        const ExtendedLogMessage & msg_ext, const std::shared_ptr<InternalTextLogsQueue> & logs_queue, ThreadName msg_thread_name);

    MapWithMemoryTracking<std::string, ChannelPtr> channels;
    std::weak_ptr<DB::TextLogQueue> text_log;
    std::atomic<int> text_log_max_priority = 0;
    std::atomic<bool> stop_logging = false;
};

struct OwnRunnableForChannel;
struct OwnRunnableForTextLog;

class AsyncLogMessage;
using AsyncLogMessagePtr = std::shared_ptr<AsyncLogMessage>;

/// Like OwnSplitChannel but logs on background threads — one per channel (plus text_log), to preserve order; internalTextLogsQueue (--send-logs-level) is still written synchronously.
class OwnAsyncSplitChannel final : public OwnSplitChannelBase, public boost::noncopyable
{
public:
    explicit OwnAsyncSplitChannel(size_t async_queue_size_);
    ~OwnAsyncSplitChannel() override;

    void open() override;
    /// Best-effort: reports a failed thread join to stderr and returns. For shutdown paths
    /// (destructor, `Loggers::stopLogging`), where there is nothing better to do with the error.
    void close() override;
    /// Fail-closed variant of `close`: propagates a failed thread join. For callers whose correctness
    /// depends on no logging thread surviving, e.g. quiescing the process around `remapExecutable`.
    void closeAndJoinThreads();

    void log(const Poco::Message & msg) override;
    void log(Poco::Message && msg) override;
    void runChannel(size_t i);
    void runTextLog();

    void setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value) override;
    void addChannel(
        ChannelPtr channel,
        const std::string & name,
        int level,
        const ProfileEvents::Event & event_on_passed_message_,
        const ProfileEvents::Event & event_on_dropped_message_) override;

    void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority) override;
    void setLevel(const std::string & name, int level) override;

    void flushTextLogs();

    AsyncLogQueueSizes getAsynchronousMetrics();

private:
    /// One channel's queue: lock-free bounded MPSC with drop-on-overflow accounting; producers are `log` callers, the single consumer is the background thread feeding the channel (or text_log).
    struct LogQueue : boost::noncopyable
    {
        LogQueue(
            size_t max_size,
            const ProfileEvents::Event & event_on_passed_message_,
            const ProfileEvents::Event & event_on_dropped_message_)
            : messages(max_size)
            , event_on_passed_message(event_on_passed_message_)
            , event_on_dropped_message(event_on_dropped_message_)
        {
        }

        /// Fixed power-of-two capacity, slots preallocated; new messages are dropped on overflow.
        NonblockingBoundedQueue<AsyncLogMessagePtr> messages;
        const ProfileEvents::Event & event_on_passed_message;
        const ProfileEvents::Event & event_on_dropped_message;
        /// Overflow drops so far: incremented by producers, reported and reset by the consumer.
        std::atomic<size_t> dropped_messages = 0;
    };

    /// Pushes the message into the queue. If the queue is full, drops the message and counts the drop.
    static void enqueueMessage(LogQueue & queue, AsyncLogMessagePtr message);

    /// Publishes the current flush request counter as completed and wakes the waiters.
    void releaseWaitingFlushers();

    std::atomic<bool> is_open = false;
    const size_t async_queue_size;

    /// Each channel has a different queue, and each one a single thread handling it
    MapWithMemoryTracking<std::string, ChannelPtr> name_to_channels;
    VectorWithMemoryTracking<OwnFormattingChannel *> channels;
    VectorWithMemoryTracking<std::unique_ptr<LogQueue>> queues;
    VectorWithMemoryTracking<std::unique_ptr<Poco::Thread>> threads;
    VectorWithMemoryTracking<std::unique_ptr<OwnRunnableForChannel>> runnables;

    /// system.text_log does not have a channel, but it's also async
    LogQueue text_log_queue;
    /// Flush handshake. A flushTextLogs caller increments text_log_flush_requested and waits until
    /// text_log_flush_completed reaches its request number. The text log thread loads the request counter,
    /// only then samples the queue's enqueue position, drains up to it, and publishes the request number it
    /// served. Sampling after the load makes the boundary exact: the acquire load of the request counter
    /// synchronizes with every requester's increment (an RMW extends the release sequence), so every record
    /// enqueued before a served request is covered by the sampled position (read-write coherence).
    /// This covers records pushed by threads other than the requester too. The only observable sense in
    /// which such a push precedes the request is a happens-before chain from the completed tryPush to the
    /// requester's increment (the requester learned of the record through some synchronization — a query
    /// response, a mutex, a pipe); prepending that chain to the edge above orders the push's enqueue_pos
    /// update before the sample, so coherence still applies. A push with no such chain is concurrent with
    /// the request, and no data-race-free program can tell whether it "was accepted first" — a mutex-based
    /// boundary would make no stronger observable promise.
    /// Distinct request numbers also keep concurrent flushers exact: a flush that starts during another
    /// flush gets a higher number and is not released by the earlier drain, whose boundary was sampled
    /// before its records were pushed.
    std::atomic<UInt64> text_log_flush_requested = 0;
    std::atomic<UInt64> text_log_flush_completed = 0;
    std::unique_ptr<Poco::Thread> text_log_thread;
    std::unique_ptr<OwnRunnableForTextLog> text_log_runnable;
    std::weak_ptr<DB::TextLogQueue> text_log;
    std::atomic<int> text_log_max_priority = 0;
};


struct OwnRunnableForChannel : public Poco::Runnable
{
    OwnRunnableForChannel(OwnAsyncSplitChannel & split_, size_t i_)
        : split(split_)
        , i(i_)
    {
    }
    ~OwnRunnableForChannel() override = default;

    void run() override { split.runChannel(i); }

private:
    OwnAsyncSplitChannel & split;
    size_t i;
};

struct OwnRunnableForTextLog : public Poco::Runnable
{
    explicit OwnRunnableForTextLog(OwnAsyncSplitChannel & split_)
        : split(split_)
    {
    }
    ~OwnRunnableForTextLog() override = default;

    void run() override { split.runTextLog(); }

private:
    OwnAsyncSplitChannel & split;
};
};
