#include <Dictionaries/LibraryDictionarySourceExternal.h>
#include <common/logger_useful.h>

namespace
{
    const char DICT_LOGGER_NAME[] = "LibraryDictionarySourceExternal";
    void trace(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.trace()) {
            logger.trace(msg);
        }
    }

    void debug(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.debug()) {
            logger.debug(msg);
        }
    }

    void information(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.information()) {
            logger.information(msg);
        }
    }

    void notice(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.notice()) {
            logger.notice(msg);
        }
    }

    void warning(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.warning()) {
            logger.warning(msg);
        }
    }

    void error(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.error()) {
            logger.error(msg);
        }
    }

    void critical(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.critical()) {
            logger.critical(msg);
        }
    }

    void fatal(ClickHouseLibrary::CString msg)
    {
        auto & logger = Logger::get(DICT_LOGGER_NAME);
        if (logger.fatal()) {
            logger.fatal(msg);
        }
    }
}


void ClickHouseLibrary::initDictLogger(CLogger * logger) {
    logger->trace = trace;
    logger->debug = debug;
    logger->information = information;
    logger->notice = notice;
    logger->warning = warning;
    logger->error = error;
    logger->critical = critical;
    logger->fatal = fatal;
}
