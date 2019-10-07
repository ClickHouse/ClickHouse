#include "Logger.h"

#include <Common/CurrentThread.h>

namespace
{

std::unordered_map<UInt8, Poco::Message::Priority> level_to_prio = {
    {FATAL,  Poco::Message::PRIO_FATAL},
    {ABORT,  Poco::Message::PRIO_FATAL},
    {EXCEPT, Poco::Message::PRIO_ERROR},
    {ERROR,  Poco::Message::PRIO_ERROR},
    {WARN,   Poco::Message::PRIO_WARNING},
    {INFO,   Poco::Message::PRIO_INFORMATION},
    {DEBUG,  Poco::Message::PRIO_DEBUG},
    {TRACE,  Poco::Message::PRIO_TRACE},
};

}  // namespace

Logger::Logger(UInt8 level_, const char * filename, const char * funcname, int linenum, Poco::Logger & log_)
    : level(level_), file(filename), func(funcname), line(linenum), log(log_)
{
    if (level == EXCEPT)
        stream << func << ": ";
}

Logger::~Logger()
{
    const bool is_clients_log = CurrentThread::getGroup() && CurrentThread::getGroup()->client_logs_level >= level;
    if (level != NONE && (log->is(level_to_prio[level]) || is_clients_log))
    {
        if (auto channel = log->getChannel())
        {
            if (level == EXCEPT)
                stream << getCurrentExceptionMessage(true);

            auto file_function = file + ";" + func;
            Poco::Message poco_message(log->name(), stream.str(), level_to_prio[level], file_function.c_str(), line);
            channel->log(poco_message);
        }
    }
}
