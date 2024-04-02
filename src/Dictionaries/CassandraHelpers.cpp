#include <Dictionaries/CassandraHelpers.h>

#if USE_CASSANDRA
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
extern const int CASSANDRA_INTERNAL_ERROR;
}

void cassandraCheck(CassError code)
{
    if (code != CASS_OK)
        throw Exception(ErrorCodes::CASSANDRA_INTERNAL_ERROR,
            "Cassandra driver error {}: {}",
            std::to_string(code),
            cass_error_desc(code));
}


void cassandraWaitAndCheck(CassFuturePtr & future)
{
    auto code = cass_future_error_code(future);     /// Waits if not ready
    if (code == CASS_OK)
        return;

    /// `future` owns `message` and will free it on destruction
    const char * message;
    size_t message_len;
    cass_future_error_message(future, &message, & message_len);

    throw Exception(ErrorCodes::CASSANDRA_INTERNAL_ERROR,
        "Cassandra driver error {}: {}: {}",
        std::to_string(code),
        cass_error_desc(code),
        message);
}

static std::once_flag setup_logging_flag;

void setupCassandraDriverLibraryLogging(CassLogLevel level)
{
    std::call_once(setup_logging_flag, [level]()
    {
        Poco::Logger * logger = &Poco::Logger::get("CassandraDriverLibrary");
        cass_log_set_level(level);
        if (level != CASS_LOG_DISABLED)
            cass_log_set_callback(cassandraLogCallback, logger);
    });
}

void cassandraLogCallback(const CassLogMessage * message, void * data)
{
    Poco::Logger * logger = static_cast<Poco::Logger *>(data);
    if (message->severity == CASS_LOG_CRITICAL || message->severity == CASS_LOG_ERROR)
        LOG_ERROR(logger, fmt::runtime(message->message));
    else if (message->severity == CASS_LOG_WARN)
        LOG_WARNING(logger, fmt::runtime(message->message));
    else if (message->severity == CASS_LOG_INFO)
        LOG_INFO(logger, fmt::runtime(message->message));
    else if (message->severity == CASS_LOG_DEBUG)
        LOG_DEBUG(logger, fmt::runtime(message->message));
    else if (message->severity == CASS_LOG_TRACE)
        LOG_TRACE(logger, fmt::runtime(message->message));
}

}

#endif
