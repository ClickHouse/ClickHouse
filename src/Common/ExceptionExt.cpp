#include <Core/LogsLevel.h>
#include <Common/Exception.h>
#include <Common/ExceptionExt.h>
#include <Common/LockMemoryExceptionInThread.h>

namespace DB
{

void tryLogCurrentException(LogFrequencyLimiterImpl && logger, const std::string & start_of_message, LogsLevel level)
{
    /// Explicitly block MEMORY_LIMIT_EXCEEDED
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    tryLogCurrentExceptionImpl(std::move(logger), start_of_message, level);
}

}
