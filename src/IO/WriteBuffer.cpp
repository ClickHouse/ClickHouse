#include "WriteBuffer.h"

#include <Common/logger_useful.h>

namespace DB
{

/// Calling finalize() in the destructor of derived classes is a bad practice.
/// This causes objects to be left on the remote FS when a write operation is rolled back.
/// Do call finalize() explicitly, before this call you have no guarantee that the file has been written
WriteBuffer::~WriteBuffer()
{
    // That destructor could be call with finalized=false in case of exceptions
    if (count() > 0 && !finalized && !canceled)
    {
        /// It is totally OK to destroy instance without finalization when an exception occurs
        /// However it is suspicious to destroy instance without finalization at the green path
        if (!std::uncaught_exceptions() && std::current_exception() == nullptr)
        {
            LoggerPtr log = getLogger("WriteBuffer");
            LOG_ERROR(
                log,
                "WriteBuffer is neither finalized nor canceled when destructor is called. "
                "No exceptions in flight are detected. "
                "The file might not be written at all or might be truncated. "
                "Stack trace: {}",
                StackTrace().toString());
            chassert(false && "WriteBuffer is not finalized in destructor.");
        }
    }
}

void WriteBuffer::cancel() noexcept
{
    if (canceled || finalized)
        return;

    LockMemoryExceptionInThread lock(VariableContext::Global);
    cancelImpl();
    canceled = true;
}
}
