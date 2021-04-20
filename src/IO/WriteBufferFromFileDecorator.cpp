#include "WriteBufferFromFileDecorator.h"

#include <IO/WriteBuffer.h>

namespace DB
{

WriteBufferFromFileDecorator<WriteBuffer>::WriteBufferFromFileDecorator(std::unique_ptr<WriteBuffer> impl_)
    : WriteBufferFromFileBase(0, nullptr, 0), impl(std::move(impl_))
{
    swap(*impl);
}

WriteBufferFromFileDecorator<WriteBuffer>::finalize()
{
    if (finalized)
        return;

    next();
    impl->finalize();

    finalizeImpl();

    finalized = true;
}

WriteBufferFromFileDecorator<WriteBuffer>::~WriteBufferFromFileDecorator()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void WriteBufferFromFileDecorator<WriteBuffer>::sync()
{
    impl->sync();
}

std::string WriteBufferFromFileDecorator<WriteBuffer>::getFileName() const
{
    if (auto & buffer = dynamic_cast<WriteBufferFromFileBase&>(*impl))
        return buffer.getFileName();
    return std::string();
}

void WriteBufferFromFileDecorator<WriteBuffer>::nextImpl()
{
    swap(*impl);
    impl->next();
    swap(*impl);
}

}
