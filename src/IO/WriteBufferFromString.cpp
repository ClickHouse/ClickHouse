#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

/// It is safe to make them autofinalizable.
WriteBufferFromString::~WriteBufferFromString()
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

WriteBufferFromOwnString::~WriteBufferFromOwnString()
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
