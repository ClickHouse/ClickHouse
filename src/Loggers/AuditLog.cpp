#include <Loggers/AuditLog.h>
#include <Loggers/Loggers.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/OwnSplitChannel.h>
#include <Common/Exception.h>
#include <Common/IO.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>

#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>


namespace ProfileEvents
{
extern const Event AsyncLoggingAuditFileLogDroppedMessages;
extern const Event AsyncLoggingAuditFileLogTotalMessages;
}

namespace DB
{

namespace fs = std::filesystem;

namespace
{

void createAuditLogDirectory(const std::string & file)
{
    auto path = fs::path(file).parent_path();
    if (path.empty())
        return;
    fs::create_directories(path);
}

std::atomic<AuditLog *> global_audit_log{nullptr};
std::atomic<bool> allow_audit_logging{false};

}

AuditLog * getAuditLog()
{
    if (!allow_audit_logging.load(std::memory_order_acquire))
        return nullptr;
    return global_audit_log.load(std::memory_order_acquire);
}

void setGlobalAuditLog(AuditLog * log)
{
    global_audit_log.store(log, std::memory_order_release);
}

void setAuditLoggingEnabled(bool enabled)
{
    allow_audit_logging.store(enabled, std::memory_order_release);
}

bool hasGlobalAuditLog()
{
    return global_audit_log.load(std::memory_order_acquire) != nullptr;
}


struct AuditLogRunnable : public Poco::Runnable
{
    explicit AuditLogRunnable(AuditLog & audit_log_) : audit_log(audit_log_) {}
    void run() override { audit_log.run(); }

private:
    AuditLog & audit_log;
};

AuditLog::AuditLog(bool async, size_t queue_size)
    : is_async(async)
{
    if (is_async)
        queue = std::make_unique<AsyncLogMessageQueue>(
            queue_size,
            ProfileEvents::AsyncLoggingAuditFileLogTotalMessages,
            ProfileEvents::AsyncLoggingAuditFileLogDroppedMessages);
}

AuditLog::~AuditLog()
{
    if (is_open)
        close();
}

void AuditLog::configure(Poco::Util::AbstractConfiguration & config, const std::string & auditlog_path)
{
    if (auditlog_path.empty())
        return;

    createAuditLogDirectory(auditlog_path);

    file_channel = new Poco::FileChannel;
    file_channel->setProperty(Poco::FileChannel::PROP_PATH, fs::weakly_canonical(auditlog_path));
    file_channel->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.rotation", config.getRawString("logger.size", "100M")));
    file_channel->setProperty(Poco::FileChannel::PROP_ARCHIVE, "timestamp");
    file_channel->setProperty(Poco::FileChannel::PROP_TIMES, "local");
    file_channel->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
    file_channel->setProperty(Poco::FileChannel::PROP_STREAMCOMPRESS, config.getRawString("logger.stream_compress", "false"));
    file_channel->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
    file_channel->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
    file_channel->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));

    Poco::AutoPtr<OwnPatternFormatter> pf = getFormatForChannel(config, "auditlog");
    formatting_channel = std::make_shared<OwnFormattingChannel>(pf, file_channel);
}

void AuditLog::open()
{
    if (!formatting_channel)
        return;

    formatting_channel->open();
    is_open = true;

    if (is_async)
    {
        writer_runnable = std::make_unique<AuditLogRunnable>(*this);
        writer_thread = std::make_unique<Poco::Thread>();
        writer_thread->start(*writer_runnable);
    }
}

void AuditLog::close()
{
    if (!is_open)
        return;

    is_open = false;

    if (is_async && queue)
    {
        queue->wakeUp();
        if (writer_thread && writer_thread->isRunning())
            writer_thread->join();
        writer_thread.reset();
        writer_runnable.reset();
    }

    if (formatting_channel)
        formatting_channel->close();
}

void AuditLog::closeFile()
{
    if (file_channel)
        file_channel->close();
}

void AuditLog::write(std::string message)
{
    if (!is_open || !formatting_channel)
        return;

    auto async_msg = std::make_shared<AsyncLogMessage>(
        Poco::Message("AUDIT", message, Poco::Message::PRIO_NOTICE));

    if (!is_async)
    {
        formatting_channel->logExtended(async_msg->msg_ext);
        return;
    }

    queue->enqueueMessageBlocking(std::move(async_msg));
}

size_t AuditLog::getQueueSize() const
{
    if (!is_async || !queue)
        return 0;
    return queue->getCurrentMessageSize();
}

void AuditLog::run()
{
    setThreadName(ThreadName::ASYNC_AUDIT_LOG);
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    while (is_open)
    {
        try
        {
            auto notification = queue->waitDequeueMessage();
            if (!notification)
                continue;

            if (const auto * own_notification = dynamic_cast<const AsyncLogMessage *>(notification.get()))
                formatting_channel->logExtended(own_notification->msg_ext);

            if (queue->request_flush)
            {
                auto batch = queue->getCurrentQueueAndClear();
                while (!batch.empty())
                {
                    auto & msg = batch.front();
                    if (const auto * own_msg = dynamic_cast<const AsyncLogMessage *>(msg.get()))
                        formatting_channel->logExtended(own_msg->msg_ext);
                    batch.pop_front();
                }
                queue->request_flush = false;
            }
        }
        catch (...)
        {
            const std::string & exception_message = getCurrentExceptionMessage(true);
            writeRetry(STDERR_FILENO, "Cannot write audit log message: ");
            writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
            writeRetry(STDERR_FILENO, "\n");
        }
    }

    if (queue)
    {
        auto remaining = queue->getCurrentQueueAndClear();
        for (auto & msg : remaining)
        {
            try
            {
                if (const auto * own_msg = dynamic_cast<const AsyncLogMessage *>(msg.get()))
                    formatting_channel->logExtended(own_msg->msg_ext);
            }
            catch (...)
            {
                const std::string & exception_message = getCurrentExceptionMessage(true);
                writeRetry(STDERR_FILENO, "Cannot log message in AuditLog: ");
                writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
                writeRetry(STDERR_FILENO, "\n");
            }
        }
    }
}

}
