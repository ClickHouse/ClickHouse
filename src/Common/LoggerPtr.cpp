#include <Common/LoggerPtr.h>

struct LoggerDeleter
{
    void operator()(const Poco::Logger * logger)
    {
        Poco::Logger::destroy(logger->name());
    }
};

LoggerPtr getLogger(const std::string & name)
{
    Poco::Logger * logger_raw_ptr = &Poco::Logger::get(name);
    return std::shared_ptr<Poco::Logger>(logger_raw_ptr, LoggerDeleter());
}
