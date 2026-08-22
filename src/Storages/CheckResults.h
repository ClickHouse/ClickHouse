#pragma once

#include <base/types.h>
#include <vector>

namespace DB
{

/// Result of CHECK TABLE query for single part of table
struct CheckResult
{
    /// Part name for merge tree or file name for simpler tables
    String fs_path;
    /// Does check passed
    bool success = false;
    /// Failure message if any
    String failure_message;

    CheckResult() = default;
    CheckResult(const String & fs_path_, bool success_, String failure_message_)
        : fs_path(fs_path_), success(success_), failure_message(failure_message_)
    {}
};

}
