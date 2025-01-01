#include "WriteBuffer.h"

#include <Common/StackTrace.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Calling finalize() in the destructor of derived classes is a bad practice.
/// This causes objects to be left on the remote FS when a write operation is rolled back.
/// Do call finalize() explicitly, before this call you have no guarantee that the file has been written
WriteBuffer::~WriteBuffer()
{
    // That destructor could be call with finalized=false in case of exceptions
    if (!finalized && !canceled && !isStackUnwinding())
    {
        LoggerPtr log = getLogger("WriteBuffer");
        LOG_ERROR(
            log,
            "WriteBuffer is neither finalized nor canceled when destructor is called. "
            "No exceptions in flight are detected. "
            "The file might not be written at all or might be truncated."
            "Stack trace: {}",
            StackTrace().toString());
        chassert(false && "WriteBuffer is neither finalized nor canceled in destructor.");
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

void WriteBuffer::finalize()
{
    if (finalized)
        return;

    if (canceled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot finalize buffer after cancellation.");

    LockMemoryExceptionInThread lock(VariableContext::Global);
    try
    {
        finalizeImpl();
        finalized = true;
    }
    catch (...)
    {
        pos = working_buffer.begin();

        cancel();

        throw;
    }
}
}
