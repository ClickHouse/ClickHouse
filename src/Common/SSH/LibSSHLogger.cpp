#include "Common/logger_useful.h"
#include "clibssh.h"

#include "LibSSHLogger.h"

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
            case SSH_LOG_PACKET:
            case SSH_LOG_FUNCTIONS:
                LOG_TRACE(logger, "{}", buffer);
                break;
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
