#include "LibraryInterface.h"

#include <common/logger_useful.h>

namespace
{
const char DICT_LOGGER_NAME[] = "LibraryDictionarySourceExternal";
}

namespace ClickHouseLibrary
{

std::string_view LIBRARY_CREATE_NEW_FUNC_NAME = "ClickHouseDictionary_v3_libNew";
std::string_view LIBRARY_CLONE_FUNC_NAME = "ClickHouseDictionary_v3_libClone";
std::string_view LIBRARY_DELETE_FUNC_NAME = "ClickHouseDictionary_v3_libDelete";

std::string_view LIBRARY_DATA_NEW_FUNC_NAME = "ClickHouseDictionary_v3_dataNew";
std::string_view LIBRARY_DATA_DELETE_FUNC_NAME = "ClickHouseDictionary_v3_dataDelete";

std::string_view LIBRARY_LOAD_ALL_FUNC_NAME = "ClickHouseDictionary_v3_loadAll";
std::string_view LIBRARY_LOAD_IDS_FUNC_NAME = "ClickHouseDictionary_v3_loadIds";
std::string_view LIBRARY_LOAD_KEYS_FUNC_NAME = "ClickHouseDictionary_v3_loadKeys";

std::string_view LIBRARY_IS_MODIFIED_FUNC_NAME = "ClickHouseDictionary_v3_isModified";
std::string_view LIBRARY_SUPPORTS_SELECTIVE_LOAD_FUNC_NAME = "ClickHouseDictionary_v3_supportsSelectiveLoad";

void log(LogLevel level, CString msg)
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

}
