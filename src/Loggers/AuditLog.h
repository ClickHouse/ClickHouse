#pragma once

#include <unistd.h>

#include <Common/MemoryTrackerBlockerInThread.h>
#include <Loggers/OwnSplitChannel.h>

#include <fmt/format.h>


#define LOG_AUDIT(audit_log_ptr, ...) do                                                                    \
{                                                                                                           \
    auto * _audit_log_instance = (audit_log_ptr);                                                           \
    if (!_audit_log_instance)                                                                                \
        break;                                                                                              \
    MemoryTrackerBlockerInThread _audit_mem_block(VariableContext::Global);                                  \
    try                                                                                                     \
    {                                                                                                       \
        _audit_log_instance->write(fmt::format(__VA_ARGS__));                                               \
    }                                                                                                       \
    catch (...)                                                                                              \
    {                                                                                                       \
        (void)::write(STDERR_FILENO, static_cast<const void *>("Failed to write audit log message\n"), 34); \
    }                                                                                                       \
} while (false)
