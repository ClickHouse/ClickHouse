#include <Common/Exception.h>
#include <IO/WriteBufferFromTrackedString.h>

namespace DB
{

/// It is safe to make them autofinalizable.
WriteBufferFromTrackedString::~WriteBufferFromTrackedString()
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
