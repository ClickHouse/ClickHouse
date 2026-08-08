#pragma once

#include <base/strong_typedef.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <vector>

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
using AsyncLogQueueSizes = std::vector<AsyncLogQueueSize>;

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

    std::map<std::string, ChannelPtr> channels;
    std::weak_ptr<DB::TextLogQueue> text_log;
    std::atomic<int> text_log_max_priority = 0;
    std::atomic<bool> stop_logging = false;
};

struct OwnRunnableForChannel;
struct OwnRunnableForTextLog;

class AsyncLogMessage;
using AsyncLogMessagePtr = std::shared_ptr<AsyncLogMessage>;

class AsyncLogMessageQueue
{
public:
    explicit AsyncLogMessageQueue(
        size_t max_size_, const ProfileEvents::Event & event_on_passed_message_, const ProfileEvents::Event & event_on_drop_message_);

    using Queue = std::deque<AsyncLogMessagePtr>;

    /// Enqueues a single message notification
    void enqueueMessage(AsyncLogMessagePtr message);

    /// Waits for a message notification to be dequeued and returns it. It might return an empty notification if wakeUp() was called
    /// or a spurious wakeup occurs
    AsyncLogMessagePtr waitDequeueMessage();

    /// Gets the full queue including all pending notifications and clears it. It might return an empty queue if no messages were available
    Queue getCurrentQueueAndClear();

    /// Wakes up any threads waiting for a message notification.
    void wakeUp();

    /// Gets the current size of the queue.
    size_t getCurrentMessageSize();

    std::atomic<bool> request_flush = false;

private:
    Queue message_queue;
    std::condition_variable condition;
    const ProfileEvents::Event & event_on_passed_message;
    const ProfileEvents::Event & event_on_drop_message;
    /// Default queue limit, to prevent memory overflow
    const size_t max_size = 10000;
    size_t dropped_messages = 0;
    std::mutex mutex;
};


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
    std::atomic<bool> is_open = false;
    const size_t async_queue_size;

    /// Each channel has a different queue, and each one a single thread handling it
    std::map<std::string, ChannelPtr> name_to_channels;
    std::vector<OwnFormattingChannel *> channels;
    std::vector<std::unique_ptr<AsyncLogMessageQueue>> queues;
    std::vector<std::unique_ptr<Poco::Thread>> threads;
    std::vector<std::unique_ptr<OwnRunnableForChannel>> runnables;

    /// system.text_log does not have a channel, but it's also async
    AsyncLogMessageQueue text_log_queue;
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
