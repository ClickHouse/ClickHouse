#include "ExternalDictionaryLibraryAPI.h"

#include <Common/logger_useful.h>

namespace
{
const char DICT_LOGGER_NAME[] = "LibraryDictionarySourceExternal";
}

void ExternalDictionaryLibraryAPI::log(LogLevel level, CString msg)
{
    auto & logger = Poco::Logger::get(DICT_LOGGER_NAME);
    switch (level)
    {
        case LogLevel::TRACE:
            if (logger.trace())
                logger.trace(msg);
            break;
        case LogLevel::DEBUG:
            if (logger.debug())
                logger.debug(msg);
            break;
        case LogLevel::INFORMATION:
            if (logger.information())
                logger.information(msg);
            break;
        case LogLevel::NOTICE:
            if (logger.notice())
                logger.notice(msg);
            break;
        case LogLevel::WARNING:
            if (logger.warning())
                logger.warning(msg);
            break;
        case LogLevel::ERROR:
            if (logger.error())
                logger.error(msg);
            break;
        case LogLevel::CRITICAL:
            if (logger.critical())
                logger.critical(msg);
            break;
        case LogLevel::FATAL:
            if (logger.fatal())
                logger.fatal(msg);
            break;
    }
}
