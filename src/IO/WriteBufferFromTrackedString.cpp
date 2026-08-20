#include <Common/Exception.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>

namespace DB
{

/// It is safe to make them autofinalizable.
WriteBufferFromStringWithMemoryTracking::~WriteBufferFromStringWithMemoryTracking()
{
    try
    {
        if (!this->finalized && !this->canceled)
            this->finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
