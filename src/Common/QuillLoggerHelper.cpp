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
    if (level == "none")
        return quill::LogLevel::None;
    else if (level == "fatal")
        return quill::LogLevel::Critical;
    else if (level == "critical")
        return quill::LogLevel::Critical;
    else if (level == "error")
        return quill::LogLevel::Error;
    else if (level == "warning")
        return quill::LogLevel::Warning;
    else if (level == "notice")
        return quill::LogLevel::Notice;
    else if (level == "information")
        return quill::LogLevel::Info;
    else if (level == "debug")
        return quill::LogLevel::Debug;
    else if (level == "trace")
        return quill::LogLevel::TraceL1;
    else if (level == "test")
        return quill::LogLevel::TraceL2;
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not a valid log level {}", level);
}
}
