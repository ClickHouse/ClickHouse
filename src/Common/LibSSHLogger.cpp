#include "config.h"

#if USE_SSH
#    include <Common/logger_useful.h>
#    include <Common/LibSSHLogger.h>
#    include <Common/clibssh.h>

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace ssh
{

namespace
{

void libssh_logger_callback(int priority, const char *, const char * buffer, void *)
{
    Poco::Logger * logger = &Poco::Logger::get("LibSSH");

    switch (priority)
    {
        case SSH_LOG_NOLOG:
            break;
        case SSH_LOG_WARNING:
            LOG_WARNING(logger, "{}", buffer);
            break;
        case SSH_LOG_PROTOCOL:
            LOG_TRACE(logger, "{}", buffer);
            break;
        case SSH_LOG_PACKET:
            LOG_TEST(logger, "{}", buffer);
            break;
        case SSH_LOG_FUNCTIONS:
            LOG_TEST(logger, "{}", buffer);
            break;
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown logger level received from the libssh: {}", priority);
    }
}

}

namespace libsshLogger
{

void initialize()
{
    ssh_set_log_callback(libssh_logger_callback);
    ssh_set_log_level(SSH_LOG_FUNCTIONS); // Set the maximum log level
}

}

}

#else

namespace ssh
{

namespace libsshLogger
{

void initialize() {}

}

}

#endif
