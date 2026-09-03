#include <Columns/IColumn.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnSplitChannel.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/IO.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/setThreadName.h>

#include <Poco/Message.h>

#include <base/sleep.h>
#include <base/scope_guard.h>

#if defined(MEMORY_SANITIZER)
#include <sanitizer/msan_interface.h>
#endif


namespace ProfileEvents
{
extern const Event AsyncLoggingTextLogDroppedMessages;
extern const Event AsyncLoggingTextLogTotalMessages;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

void OwnSplitChannel::open()
{
    stop_logging = false;
}

void OwnSplitChannel::close()
{
    stop_logging = true;
}

void OwnSplitChannel::log(const Poco::Message & msg)
{
    log(Poco::Message(msg));
}

void OwnSplitChannel::log(Poco::Message && msg)
{
#if defined(MEMORY_SANITIZER)
    {
        auto fmt = msg.getFormatString();
        __msan_check_mem_is_initialized(&fmt, sizeof(fmt));
        if (fmt.data())
            __msan_check_mem_is_initialized(fmt.data(), fmt.size());
    }
#endif
    if (stop_logging)
        return;

    const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
    if (channels.empty() && (logs_queue == nullptr && !logs_queue->isNeeded(msg.getPriority(), msg.getSource())))
        return;

    if (const auto & masker = SensitiveDataMasker::getInstance())
    {
        auto message_text = msg.getText();
        auto matches = masker->wipeSensitiveDataThrow(message_text);
        if (matches > 0)
        {
            msg.setText(message_text);
            logSplit(ExtendedLogMessage::getFrom(msg), logs_queue, getThreadName());
            return;
        }
    }

    logSplit(ExtendedLogMessage::getFrom(msg), logs_queue, getThreadName());
}

namespace
{

/// Sleep between polls of an empty queue (consumers poll instead of futex-waiting, keeping pushes cheap).
constexpr size_t sleep_on_empty_queue_ms = 10;

/// Pops one message for a flush (null if empty), waiting out a head slot still being written so it isn't skipped.
AsyncLogMessagePtr dequeueMessageForFlush(NonblockingBoundedQueue<AsyncLogMessagePtr> & messages)
{
    AsyncLogMessagePtr message;
    while (!messages.tryPop(message))
    {
        if (messages.size() == 0)
            return nullptr;
        sleepForMilliseconds(10);
    }
    return message;
}

void pushExtendedMessageToInternalTCPTextLogQueue(
    const ExtendedLogMessage & msg_ext, const std::shared_ptr<InternalTextLogsQueue> & logs_queue)
{
    const Poco::Message & msg = *msg_ext.base;
    MutableColumns columns = InternalTextLogsQueue::getSampleColumns();

    size_t i = 0;
    columns[i++]->insert(msg_ext.time_seconds);
    columns[i++]->insert(msg_ext.time_microseconds);
    columns[i++]->insert(DNSResolver::instance().getHostName());
    columns[i++]->insert(msg_ext.query_id);
    columns[i++]->insert(msg_ext.thread_id);
    columns[i++]->insert(static_cast<Int64>(msg.getPriority()));
    columns[i++]->insert(msg.getSource());
    columns[i++]->insert(msg.getText());

    [[maybe_unused]] bool push_result = logs_queue->emplace(std::move(columns));
}

void logToSystemTextLogQueue(
    const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked,
    const ExtendedLogMessage & msg_ext,
    ThreadName msg_thread_name)
{
    const Poco::Message & msg = *msg_ext.base;
    text_log_locked->add([&](TextLogElement & elem)
    {
    elem.event_time = msg_ext.time_seconds;
    elem.event_time_microseconds = msg_ext.time_in_microseconds;

    elem.thread_name = msg_thread_name;
    elem.thread_id = msg_ext.thread_id;

    elem.query_id = msg_ext.query_id;

    elem.message = msg.getText();
    elem.logger_name = msg.getSource();
    elem.level = msg.getPriority();
    elem.source_file = msg.getSourceFile();

    elem.source_line = msg.getSourceLine();
    elem.message_format_string = msg.getFormatString();

#define SET_VALUE_IF_EXISTS(INDEX) \
    if ((INDEX) <= msg.getFormatStringArgs().size()) \
        (elem.value##INDEX) = msg.getFormatStringArgs()[(INDEX) - 1]

    SET_VALUE_IF_EXISTS(1);
    SET_VALUE_IF_EXISTS(2);
    SET_VALUE_IF_EXISTS(3);
    SET_VALUE_IF_EXISTS(4);
    SET_VALUE_IF_EXISTS(5);
    SET_VALUE_IF_EXISTS(6);
    SET_VALUE_IF_EXISTS(7);
    SET_VALUE_IF_EXISTS(8);
    SET_VALUE_IF_EXISTS(9);
    SET_VALUE_IF_EXISTS(10);

#undef SET_VALUE_IF_EXISTS
    });
}
}

void OwnSplitChannel::logSplit(
    const ExtendedLogMessage & msg_ext, const std::shared_ptr<InternalTextLogsQueue> & logs_queue, ThreadName msg_thread_name)
{
    const Poco::Message & msg = *msg_ext.base;

    try
    {
        /// Log data to child channels
        for (auto & channel : channels | std::views::values)
        {
            auto priority = channel->getPriority();
            if (priority >= msg.getPriority())
                channel->logExtended(msg_ext);
        }

        /// Log to "TCP queue" if message is not too noisy
        if (logs_queue && logs_queue->isNeeded(msg.getPriority(), msg.getSource()))
            pushExtendedMessageToInternalTCPTextLogQueue(msg_ext, logs_queue);

        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        /// Also log to system.text_log table, if message is not too noisy
        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (text_log_max_priority_loaded && msg.getPriority() <= text_log_max_priority_loaded)
        {
            logToSystemTextLogQueue(text_log_locked, msg_ext, msg_thread_name);
        }
    }
    /// It is better to catch the errors here in order to avoid
    /// breaking some functionality because of unexpected "File not
    /// found" (or similar) error.
    ///
    /// For example DistributedAsyncInsertDirectoryQueue will mark batch
    /// as broken, some MergeTree code can also be affected.
    ///
    /// Also note, that we cannot log the exception here, since this
    /// will lead to recursion, using regular tryLogCurrentException().
    /// but let's log it into the stderr at least.
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        const std::string & message = msg.getText();

        /// NOTE: errors are ignored, since nothing can be done.
        writeRetry(STDERR_FILENO, "Cannot add message to the log: ");
        writeRetry(STDERR_FILENO, message.data(), message.size());
        writeRetry(STDERR_FILENO, "\n");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}


void OwnSplitChannel::addChannel(
    ChannelPtr channel, const std::string & name, int level, const ProfileEvents::Event &, const ProfileEvents::Event &)
{
    channel->setLevel(level);
    channels.emplace(name, channel);
}

void OwnSplitChannel::addTextLog(std::shared_ptr<SystemLogQueue<TextLogElement>> log_queue, int max_priority)
{
    text_log = log_queue;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
}

void OwnSplitChannel::setLevel(const std::string & name, int level)
{
     auto it = channels.find(name);
     if (it != channels.end())
         it->second->setLevel(level);
}

void OwnSplitChannel::setChannelProperty(const std::string& channel_name, const std::string& name, const std::string& value)
{
    auto it = channels.find(channel_name);
    if (it != channels.end())
        it->second->setProperty(name, value);
}

OwnAsyncSplitChannel::OwnAsyncSplitChannel(size_t async_queue_size_)
    : async_queue_size(async_queue_size_)
    , text_log_queue(async_queue_size_, ProfileEvents::AsyncLoggingTextLogTotalMessages, ProfileEvents::AsyncLoggingTextLogDroppedMessages)
{
    if (async_queue_size_ == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Asynchronous log message queue cannot have zero size");
}

OwnAsyncSplitChannel::~OwnAsyncSplitChannel()
{
    OwnAsyncSplitChannel::close();
}

void OwnAsyncSplitChannel::open()
{
    /// If a thread fails to start, reset its pointer before tearing down: close() joins every thread it
    /// sees, and joining a never-started Poco::Thread waits forever on a completion event nobody sets.
    auto start_thread = [](std::unique_ptr<Poco::Thread> & thread, const std::string & name, Poco::Runnable & runnable)
    {
        thread = std::make_unique<Poco::Thread>(name);
        try
        {
            thread->start(runnable);
        }
        catch (...)
        {
            thread.reset();
            throw;
        }
    };

    is_open = true;
    try
    {
        if (text_log_max_priority && !text_log_thread)
            start_thread(text_log_thread, "AsyncTextLog", *text_log_runnable);

        for (size_t i = 0; i < channels.size(); i++)
        {
            if (!threads[i])
                start_thread(threads[i], "AsyncLog", *runnables[i]);
        }
    }
    catch (...)
    {
        /// Fail closed: a partially opened channel would keep accepting messages into queues that no
        /// consumer drains, silently losing diagnostics. A failed join must also propagate: unwinding
        /// with a live async logger could overlap static destruction.
        closeAndJoinThreads();
        throw;
    }
}

void OwnAsyncSplitChannel::close()
{
    try
    {
        closeAndJoinThreads();
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot close OwnAsyncSplitChannel: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");

        /// A thread whose join failed would never serve a waiting flusher, so release them here too.
        releaseWaitingFlushers();
    }
}

void OwnAsyncSplitChannel::closeAndJoinThreads()
{
    is_open = false;

    waitForActiveAsyncLoggers();

    /// The polling consumers see is_open == false on their own, wait for the same barrier before
    /// their final drain, and exit.
    if (text_log_thread)
    {
        text_log_thread->join();
        text_log_thread.reset();
    }

    for (size_t i = 0; i < channels.size(); i++)
    {
        if (threads[i])
            threads[i]->join();
        threads[i].reset();
    }

    releaseWaitingFlushers();
}

void OwnAsyncSplitChannel::waitForActiveAsyncLoggers()
{
    /// A producer that already saw `is_open` needs to recheck it after registering here. Wait for
    /// every producer which passed that recheck before the consumers take their final drain.
    /// Producers arriving after `is_open` became false deliver synchronously instead.
    size_t active = active_async_loggers.load(std::memory_order_seq_cst);
    while (active != 0)
    {
        active_async_loggers.wait(active, std::memory_order_seq_cst);
        active = active_async_loggers.load(std::memory_order_seq_cst);
    }
}

void OwnAsyncSplitChannel::releaseWaitingFlushers()
{
    /// Release any flusher still waiting: the thread that would serve it has exited. Records the final
    /// drain did not take stay queued and are drained when the channel reopens (e.g. after remapExecutable),
    /// the same as for a flush that arrives while the channel is closed.
    text_log_flush_completed.store(text_log_flush_requested.load(std::memory_order_seq_cst), std::memory_order_release);
    text_log_flush_completed.notify_all();
}

class AsyncLogMessage
{
public:
    ALWAYS_INLINE explicit AsyncLogMessage(Message && msg_)
        : msg(std::move(msg_))
        , msg_ext(ExtendedLogMessage::getFrom(msg))
        , msg_thread_name(getThreadName())
    {
        if (const auto & masker = SensitiveDataMasker::getInstance())
        {
            auto message_text = msg.getText();
            auto matches = masker->wipeSensitiveDataThrow(message_text);
            if (matches > 0)
                msg.setText(message_text);
        }
    }

    Message msg; /// Need to keep a copy until we finish logging
    ExtendedLogMessage msg_ext;
    ThreadName msg_thread_name;
};


namespace
{

/// Builds the "dropped N messages" warning, reported in the channel that dropped them.
AsyncLogMessagePtr makeDroppedMessagesWarning(size_t dropped)
{
    auto warning = std::make_shared<AsyncLogMessage>(Poco::Message(
        "AsyncLogger",
        fmt::format("We've dropped {} log messages in this channel due to queue overflow", dropped),
        Poco::Message::PRIO_WARNING));
    warning->msg_ext.query_id.clear();
    return warning;
}

}

void OwnAsyncSplitChannel::enqueueMessage(LogQueue & queue, AsyncLogMessagePtr message)
{
    ProfileEvents::incrementNoTrace(queue.event_on_passed_message);

    if (unlikely(!queue.messages.tryPush(message)))
    {
        /// Queue full: drop the message and count it; the consumer warns about drops once it catches up.
        queue.dropped_messages.fetch_add(1, std::memory_order_relaxed);
        ProfileEvents::incrementNoTrace(queue.event_on_dropped_message);
    }
}

void OwnAsyncSplitChannel::log(const Poco::Message & msg)
{
    log(Poco::Message(msg));
}

void OwnAsyncSplitChannel::log(Poco::Message && msg)
{
    try
    {
#if defined(MEMORY_SANITIZER)
        /// Catch which LOG call produces uninitialized format string bytes (STID 1478-2063: arm_msan use-of-uninitialized-value in TextLog).
        {
            auto fmt = msg.getFormatString();
            __msan_check_mem_is_initialized(&fmt, sizeof(fmt));
            if (fmt.data())
                __msan_check_mem_is_initialized(fmt.data(), fmt.size());
        }
#endif
        /// logger_useful.h skips this when the message isn't needed, so creating the AsyncLogMessage here is free.
        auto msg_priority = msg.getPriority();
        auto notification = std::make_shared<AsyncLogMessage>(std::move(msg));
        if (const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
            logs_queue && logs_queue->isNeeded(msg_priority, notification->msg.getSource()))
        {
            /// If we need to push to the TCP queue, do it now since it expects to receive all messages synchronously
            pushExtendedMessageToInternalTCPTextLogQueue(notification->msg_ext, logs_queue);
        }

        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (channels.empty() && !text_log_max_priority_loaded)
            return;

        auto log_synchronously = [&]
        {
            for (const auto & channel : channels)
            {
                if (channel->getPriority() >= msg_priority)
                    channel->logExtended(notification->msg_ext);
            }

            if (text_log_max_priority_loaded >= msg_priority)
            {
                if (const auto text_log_locked = text_log.lock())
                    logToSystemTextLogQueue(text_log_locked, notification->msg_ext, notification->msg_thread_name);
            }
        };

        /// While the channel is stopped (before open, during the quiesce window around `remapExecutable`,
        /// after close) there is no consumer thread, so deliver synchronously, as `OwnSplitChannel` does.
        /// An enqueued message could otherwise be lost forever: if the server never reopens the channel -
        /// e.g. startup fails because restarting the logging threads after the remap threw - the exception
        /// unwinding out of `Server::main` is logged into queues that nobody will ever drain.
        ///
        /// Register before the second load so `closeAndJoinThreads` cannot drain and join between that load
        /// and an async enqueue. If closing wins the race, the second load redirects this message to the
        /// synchronous path; if this producer wins, closing waits for it before the final drain.
        if (!is_open)
        {
            log_synchronously();
            return;
        }

        active_async_loggers.fetch_add(1, std::memory_order_seq_cst);
        if (!is_open)
        {
            if (active_async_loggers.fetch_sub(1, std::memory_order_seq_cst) == 1)
                active_async_loggers.notify_all();
            log_synchronously();
            return;
        }

        SCOPE_EXIT({
            if (active_async_loggers.fetch_sub(1, std::memory_order_seq_cst) == 1)
                active_async_loggers.notify_all();
        });

        for (size_t i = 0; i < queues.size(); i++)
        {
            if (channels[i]->getPriority() >= msg_priority)
                enqueueMessage(*queues[i], notification);
        }

        if (text_log_max_priority_loaded >= msg_priority)
            enqueueMessage(text_log_queue, std::move(notification));
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);

        /// NOTE: errors are ignored, since nothing can be done.
        writeRetry(STDERR_FILENO, "Failed to add message to the log queue: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::flushTextLogs()
{
    auto text_log_locked = text_log.lock();
    if (!text_log_locked)
        return;

    /// The async text-log thread services this handshake. It is disabled when text_log.level is none, and is
    /// not running while logging is stopped around
    /// remapExecutable (where the only caller is the fatal signal handler; the server accepts no connections
    /// yet) nor after shutdown, so return instead of waiting forever for a flag nobody will clear. Anything
    /// queued meanwhile is drained when the thread (re)starts.
    if (!text_log_max_priority || !is_open)
        return;

    /// Take a request number and wait until the async thread reports it as served. The thread checks the
    /// request counter between messages and after each empty-queue sleep. A flush already in progress does
    /// not release this caller: it has an older request number, and its drain boundary was sampled before
    /// this increment, so it may not cover this caller's records.
    const UInt64 my_request = text_log_flush_requested.fetch_add(1, std::memory_order_seq_cst) + 1;

    UInt64 completed = text_log_flush_completed.load(std::memory_order_acquire);
    while (completed < my_request)
    {
        /// The consumer thread may have exited (shutdown or remapExecutable) after the is_open check above.
        /// Its final drain flushes everything queued and publishes the requests it observed; bail out
        /// instead of waiting forever for a request nobody will serve.
        if (!is_open)
            return;
        text_log_flush_completed.wait(completed, std::memory_order_acquire);
        completed = text_log_flush_completed.load(std::memory_order_acquire);
    }
}

AsyncLogQueueSizes OwnAsyncSplitChannel::getAsynchronousMetrics()
{
    AsyncLogQueueSizes metrics;
    for (const auto & [name, channel] : name_to_channels)
    {
        for (size_t i = 0; i < channels.size(); i++)
        {
            if (channels[i] == channel.get())
            {
                metrics.push_back({name, queues[i]->messages.size()});
                break;
            }
        }
    }

    if (text_log.lock())
        metrics.push_back({"TextLog", text_log_queue.messages.size()});

    return metrics;
}

void OwnAsyncSplitChannel::runChannel(size_t i)
{
    DB::setThreadName(ThreadName::ASYNC_LOGGER);
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    LogQueue & queue = *queues[i];
    const auto & extended_channel = channels[i];

    auto report_dropped_messages = [&]()
    {
        if (likely(queue.dropped_messages.load(std::memory_order_relaxed) == 0))
            return;

        size_t dropped = queue.dropped_messages.exchange(0, std::memory_order_relaxed);
        try
        {
            extended_channel->logExtended(makeDroppedMessagesWarning(dropped)->msg_ext);
        }
        catch (...)
        {
            /// Don't lose the count if we failed to report it
            queue.dropped_messages.fetch_add(dropped, std::memory_order_relaxed);
            throw;
        }
    };

    auto flush_queue = [&]()
    {
        /// Exact, bounded flush boundary: drain every message enqueued before close() flipped is_open
        /// (pushes happen-before this thread's read of is_open == false) and no later arrival, so producers
        /// can't prolong the flush; dequeueMessageForFlush waits out any slot still being published.
        const size_t target = queue.messages.enqueuePosition();
        while (queue.messages.dequeuePosition() < target)
        {
            auto notif = dequeueMessageForFlush(queue.messages);
            if (!notif)
                break;
            extended_channel->logExtended(notif->msg_ext);
        }
    };

    while (is_open)
    {
        try
        {
            AsyncLogMessagePtr message;
            if (queue.messages.tryPop(message))
            {
                extended_channel->logExtended(message->msg_ext);
            }
            else
            {
                /// Empty queue: sleep, then report overflow drops here. tryPop can also fail while a producer
                /// holds an unpublished slot, so only warn once the queue is truly drained (size()==0), else
                /// the warning could overtake real messages still being enqueued.
                sleepForMilliseconds(sleep_on_empty_queue_ms);
                if (queue.messages.size() == 0)
                    report_dropped_messages();
            }
        }
        catch (...)
        {
            const std::string & exception_message = getCurrentExceptionMessage(true);
            writeRetry(STDERR_FILENO, "Cannot log message in OwnAsyncSplitChannel channel: ");
            writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
            writeRetry(STDERR_FILENO, "\n");
        }
    }

    try
    {
        /// A producer can have passed the second `is_open` check before close flipped the flag.
        /// Do not sample the final queue boundary until all such producers have published, or its
        /// message could be enqueued after this consumer exits.
        waitForActiveAsyncLoggers();

        /// Flush everything before closing and report the drops which were not reported yet.
        flush_queue();
        report_dropped_messages();
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot flush messages in OwnAsyncSplitChannel channel: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::runTextLog()
{
    DB::setThreadName(ThreadName::ASYNC_TEXT_LOG);

    auto report_dropped_messages = [&](const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        if (likely(text_log_queue.dropped_messages.load(std::memory_order_relaxed) == 0))
            return;

        size_t dropped = text_log_queue.dropped_messages.exchange(0, std::memory_order_relaxed);
        try
        {
            const auto warning = makeDroppedMessagesWarning(dropped);
            logToSystemTextLogQueue(text_log_locked, warning->msg_ext, warning->msg_thread_name);
        }
        catch (...)
        {
            /// Don't lose the count if we failed to report it
            text_log_queue.dropped_messages.fetch_add(dropped, std::memory_order_relaxed);
            throw;
        }
    };

    auto flush_queue = [&](const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        /// Exact, bounded flush boundary: SYSTEM FLUSH LOGS relies on every record accepted before the
        /// request reaching text_log before flushImpl samples the last index. This position is sampled
        /// after the request counter is loaded, so it covers every record enqueued before a served request
        /// (see the handshake comment in the header); size() would be imprecise while producers run.
        /// Drain up to it, waiting out any slot still being published. Later arrivals aren't included,
        /// so producers can't prolong the flush.
        const size_t target = text_log_queue.messages.enqueuePosition();
        while (text_log_queue.messages.dequeuePosition() < target)
        {
            auto notif = dequeueMessageForFlush(text_log_queue.messages);
            if (!notif)
                break;
            logToSystemTextLogQueue(text_log_locked, notif->msg_ext, notif->msg_thread_name);
        }
    };

    while (is_open)
    {
        try
        {
            auto text_log_locked = text_log.lock();
            if (!text_log_locked)
                return;

            /// Load the request counter before sampling the queue position in flush_queue, so the drain
            /// boundary covers every record enqueued before the requests served here (see the header).
            /// text_log_flush_completed is only written by this thread, so a relaxed read is exact.
            const UInt64 flush_requested = text_log_flush_requested.load(std::memory_order_seq_cst);
            if (text_log_flush_completed.load(std::memory_order_relaxed) < flush_requested)
            {
                flush_queue(text_log_locked);
                /// Emit the drop warning as part of the flush, before releasing the waiters, so SYSTEM FLUSH
                /// LOGS / shutdown can't miss the only record that tells users messages were dropped.
                report_dropped_messages(text_log_locked);
                text_log_flush_completed.store(flush_requested, std::memory_order_release);
                text_log_flush_completed.notify_all();
                continue;
            }

            AsyncLogMessagePtr message;
            if (text_log_queue.messages.tryPop(message))
            {
                logToSystemTextLogQueue(text_log_locked, message->msg_ext, message->msg_thread_name);
            }
            else
            {
                /// Empty queue: sleep, then report overflow drops here. tryPop can also fail while a producer
                /// holds an unpublished slot, so only warn once the queue is truly drained (size()==0), else
                /// the warning could overtake real messages still being enqueued.
                sleepForMilliseconds(sleep_on_empty_queue_ms);
                if (text_log_queue.messages.size() == 0)
                    report_dropped_messages(text_log_locked);
            }
        }
        catch (...)
        {
            const std::string & exception_message = getCurrentExceptionMessage(true);
            writeRetry(STDERR_FILENO, "Cannot log message in OwnAsyncSplitChannel text log: ");
            writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
            writeRetry(STDERR_FILENO, "\n");
        }
    }

    try
    {
        /// Flush everything still queued before closing, and report any drops not yet reported.
        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        /// See `runChannel`: the final queue boundary must be sampled only after every producer
        /// which entered the asynchronous path before close has finished publishing.
        waitForActiveAsyncLoggers();

        const UInt64 flush_requested = text_log_flush_requested.load(std::memory_order_seq_cst);
        flush_queue(text_log_locked);
        report_dropped_messages(text_log_locked);
        /// Release flushers racing with shutdown: this drain took everything they enqueued (their remaining
        /// recourse is the is_open check, close() publishes once more after the join for the late ones).
        text_log_flush_completed.store(flush_requested, std::memory_order_release);
        text_log_flush_completed.notify_all();
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot flush queue in OwnAsyncSplitChannel text log: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value)
{
    if (auto it = name_to_channels.find(channel_name); it != name_to_channels.end())
        it->second->setProperty(name, value);
}

void OwnAsyncSplitChannel::addChannel(
    ChannelPtr channel,
    const std::string & name,
    int level,
    const ProfileEvents::Event & event_on_passed_message_,
    const ProfileEvents::Event & event_on_dropped_message_)
{
    if (is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempted to register channel '{}' while the split channel is open", name);

    auto element = name_to_channels.try_emplace(name, channel);
    if (!element.second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Channel {} is already registered", name);
    channel->setLevel(level);

    channels.emplace_back(element.first->second.get());
    queues.emplace_back(std::make_unique<LogQueue>(async_queue_size, event_on_passed_message_, event_on_dropped_message_));
    threads.emplace_back(nullptr);
    const size_t i = threads.size() - 1;
    runnables.emplace_back(new OwnRunnableForChannel(*this, i));
}

void OwnAsyncSplitChannel::addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority)
{
    if (is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempted to register channel for text_log while the split channel is open");
    text_log = log_queue;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
    text_log_thread = nullptr;
    text_log_runnable = std::make_unique<OwnRunnableForTextLog>(*this);
}

void OwnAsyncSplitChannel::setLevel(const std::string & name, int level)
{
    if (auto it = name_to_channels.find(name); it != name_to_channels.end())
        it->second->setLevel(level);
}
}
