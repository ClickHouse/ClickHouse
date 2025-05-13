#pragma once

#include <Loggers/ExtendedLogChannel.h>

#include <atomic>
#include <map>
#include <memory>

#include <boost/noncopyable.hpp>

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/NotificationQueue.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>

namespace DB
{

class InternalTextLogsQueue;
template <typename>
class SystemLogQueue;
struct TextLogElement;
using TextLogQueue = SystemLogQueue<TextLogElement>;

/// Works as Poco::SplitterChannel, but performs additional work:
///  passes logs to Client via TCP interface
///  tries to use extended logging interface of child for more comprehensive logging
class OwnSplitChannel : public Poco::Channel
{
public:
    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const Poco::Message & msg) override;

    void setChannelProperty(const std::string& channel_name, const std::string& name, const std::string& value);

    /// Adds a child channel
    void addChannel(Poco::AutoPtr<Poco::Channel> channel, const std::string & name);

    void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority);

    void setLevel(const std::string & name, int level);

    void logSplit(
        const ExtendedLogMessage & msg_ext,
        const std::shared_ptr<InternalTextLogsQueue> & logs_queue,
        const std::string & thread_name,
        bool check_masker = true);

    using ChannelPtr = Poco::AutoPtr<Poco::Channel>;
    /// Handler and its pointer cast to extended interface
    using ExtendedChannelPtrPair = std::pair<ChannelPtr, ExtendedLogChannel *>;
    std::map<std::string, ExtendedChannelPtrPair> channels;

    std::weak_ptr<DB::TextLogQueue> text_log;
    std::atomic<int> text_log_max_priority = -1;
};

/// Same as OwnSplitChannel but it uses a separate thread for logging.
/// Based on AsyncChannel
class OwnAsyncSplitChannel : public Poco::Channel, public Poco::Runnable, public boost::noncopyable
{
public:
    OwnAsyncSplitChannel();
    ~OwnAsyncSplitChannel() override;

    void open() override;
    void close() override;

    void log(const Poco::Message & msg) override;
    void run() override;

    void setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value);
    void addChannel(Poco::AutoPtr<Poco::Channel> channel, const std::string & name);

    void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority);
    void setLevel(const std::string & name, int level);

private:
    OwnSplitChannel sync_channel;
    Poco::Thread thread;
    Poco::NotificationQueue queue;
};
};
