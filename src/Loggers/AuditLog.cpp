#include <Loggers/AuditLog.h>
#include <Loggers/Loggers.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Common/Exception.h>

#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>


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


AuditLog::AuditLog(bool, size_t)
{
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

}

void AuditLog::close()
{
    if (!is_open)
        return;

    is_open = false;

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

    const Poco::Message audit_message("AUDIT", message, Poco::Message::PRIO_NOTICE);
    formatting_channel->logExtended(ExtendedLogMessage::getFrom(audit_message));
}

size_t AuditLog::getQueueSize() const
{
    return 0;
}

}
