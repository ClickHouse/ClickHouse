#pragma once

namespace DB
{
enum class LogsLevel
{
    none = 0, /// Disable
    fatal,
    error,
    warning,
    information,
    debug,
    trace,
    test,
};
}
