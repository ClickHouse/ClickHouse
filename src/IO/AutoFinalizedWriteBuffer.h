#pragma once

#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

namespace DB
{

/// That class is applied in
// HTTPServerResponse. The external interface HTTPResponse forces that.
// AutoFinalizedWriteBuffer could not be inherited due to a restriction on polymorphics call in d-tor.
template <class Base>
class AutoFinalizedWriteBuffer final : public Base
{
    static_assert(std::derived_from<Base, WriteBuffer>);

public:
    using Base::Base;

    ~AutoFinalizedWriteBuffer() override
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
};

}
