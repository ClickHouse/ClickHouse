#include <Common/Exception.h>
#include <IO/WriteBufferFromStrictString.h>

namespace DB
{

/// It is safe to make them autofinalizable.
WriteBufferFromStrictString::~WriteBufferFromStrictString()
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
