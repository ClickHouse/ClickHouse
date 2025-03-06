#include <boost/algorithm/string/predicate.hpp>
#include <Common/QuillLoggerHelper.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

quill::LogLevel parseQuillLogLevel(std::string_view level)
{
    if (boost::iequals(level, "none"))
        return quill::LogLevel::None;
    else if (boost::iequals(level, "fatal"))
        return quill::LogLevel::Critical;
    else if (boost::iequals(level, "critical"))
        return quill::LogLevel::Critical;
    else if (boost::iequals(level, "error"))
        return quill::LogLevel::Error;
    else if (boost::iequals(level, "warning"))
        return quill::LogLevel::Warning;
    else if (boost::iequals(level, "notice"))
        return quill::LogLevel::Notice;
    else if (boost::iequals(level, "information"))
        return quill::LogLevel::Info;
    else if (boost::iequals(level, "debug"))
        return quill::LogLevel::Debug;
    else if (boost::iequals(level, "trace"))
        return quill::LogLevel::TraceL1;
    else if (boost::iequals(level, "test"))
        return quill::LogLevel::TraceL2;
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not a valid log level {}", level);
}
}
