#pragma once

#include <base/strong_typedef.h>

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

/// Same as OwnSplitChannel but it uses separate threads for logging.
/// Note that it uses a separate thread per each different channel (including one for text_log) instead of using a common thread pool
/// to ensure the order is kept
/// Currently logging to the internalTextLogsQueue (TCP queue for --send-logs-level) is done synchronously when log is called
class OwnAsyncSplitChannel final : public OwnSplitChannelBase, public boost::noncopyable
{
public:
    explicit OwnAsyncSplitChannel(size_t async_queue_size_);
    ~OwnAsyncSplitChannel() override;

    void open() override;
    void close() override;

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
    /// The message queue of one async logging channel: a lock-free bounded MPSC queue plus drop-on-overflow
    /// accounting. Producers are the threads calling `log`, the single consumer is the background thread
    /// passing the messages to the channel (or to the system.text_log queue).
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

        /// The capacity is fixed at construction (max_size rounded up to a power of two) and the slots are
        /// preallocated; when the consumer doesn't keep up and the queue overflows, new messages are dropped.
        NonblockingBoundedQueue<AsyncLogMessagePtr> messages;
        const ProfileEvents::Event & event_on_passed_message;
        const ProfileEvents::Event & event_on_dropped_message;
        /// The number of messages dropped due to overflow so far. Incremented by the producers;
        /// the consumer reports it with a warning message logged directly to the channel and resets it.
        std::atomic<size_t> dropped_messages = 0;
    };

    /// Pushes the message into the queue. If the queue is full, drops the message and counts the drop.
    static void enqueueMessage(LogQueue & queue, AsyncLogMessagePtr message);

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
    /// Set by flushTextLogs to request the text log thread to flush the whole queue; the thread resets it
    /// (and notifies the waiters) once done.
    std::atomic<bool> text_log_flush_requested = false;
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
