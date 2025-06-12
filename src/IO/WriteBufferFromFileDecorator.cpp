#include "WriteBufferFromFileDecorator.h"

#include <IO/WriteBuffer.h>
#include <IO/SwapHelper.h>

namespace DB
{

WriteBufferFromFileDecorator::WriteBufferFromFileDecorator(std::unique_ptr<WriteBuffer> impl_)
    : WriteBufferFromFileBase(0, nullptr, 0), impl(std::move(impl_))
{
    swap(*impl);
}

void WriteBufferFromFileDecorator::finalizeImpl()
{

    /// In case of exception in preFinalize as a part of finalize call
    /// WriteBufferFromFileDecorator.finalized is set as true
    /// but impl->finalized is remain false
    /// That leads to situation when the destructor of impl is called with impl->finalized equal false.
    if (!is_prefinalized)
        WriteBufferFromFileDecorator::preFinalize();

    {
        SwapHelper swap(*this, *impl);
        impl->finalize();
    }
}

void WriteBufferFromFileDecorator::cancelImpl() noexcept
{
    SwapHelper swap(*this, *impl);
    impl->cancel();
}

WriteBufferFromFileDecorator::~WriteBufferFromFileDecorator()
{
    /// It is not a mistake that swap is called here
    /// Swap has been called at constructor, it should be called at destructor
    /// In oreder to provide valid buffer for impl's d-tor call
    swap(*impl);
}

void WriteBufferFromFileDecorator::sync()
{
    next();

    {
        SwapHelper swap(*this, *impl);
        impl->sync();
    }
}

std::string WriteBufferFromFileDecorator::getFileName() const
{
    if (WriteBufferFromFileBase * buffer = dynamic_cast<WriteBufferFromFileBase*>(impl.get()))
        return buffer->getFileName();
    return std::string();
}

void WriteBufferFromFileDecorator::preFinalize()
{
    next();

    {
        SwapHelper swap(*this, *impl);
        impl->preFinalize();
    }

    is_prefinalized = true;
}

void WriteBufferFromFileDecorator::nextImpl()
{
    SwapHelper swap(*this, *impl);
    impl->next();
}

}
