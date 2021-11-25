#include "WriteBufferFromFileDecorator.h"

#include <IO/WriteBuffer.h>

namespace DB
{

WriteBufferFromFileDecorator::WriteBufferFromFileDecorator(std::unique_ptr<WriteBuffer> impl_)
    : WriteBufferFromFileBase(0, nullptr, 0), impl(std::move(impl_))
{
    swap(*impl);
}

void WriteBufferFromFileDecorator::finalize()
{
    if (finalized)
        return;

    next();
    impl->finalize();

    finalized = true;
}

WriteBufferFromFileDecorator::~WriteBufferFromFileDecorator()
{
    try
    {
        WriteBufferFromFileDecorator::finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void WriteBufferFromFileDecorator::sync()
{
    impl->sync();
}

std::string WriteBufferFromFileDecorator::getFileName() const
{
    if (WriteBufferFromFileBase * buffer = dynamic_cast<WriteBufferFromFileBase*>(impl.get()))
        return buffer->getFileName();
    return std::string();
}

void WriteBufferFromFileDecorator::nextImpl()
{
    swap(*impl);
    impl->next();
    swap(*impl);
}

}
