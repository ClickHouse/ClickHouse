#include <IO/ReadBufferFromFileDecorator.h>


namespace DB
{

ReadBufferFromFileDecorator::ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_)
    : impl(std::move(impl_))
{
    swap(*impl);
}


std::string ReadBufferFromFileDecorator::getFileName() const
{
    if (ReadBufferFromFileBase * buffer = dynamic_cast<ReadBufferFromFileBase *>(impl.get()))
        return buffer->getFileName();
    return std::string();
}


off_t ReadBufferFromFileDecorator::getPosition()
{
    swap(*impl);
    auto position = impl->getPosition();
    swap(*impl);
    return position;
}


off_t ReadBufferFromFileDecorator::seek(off_t off, int whence)
{
    swap(*impl);
    auto result = impl->seek(off, whence);
    swap(*impl);
    return result;
}


bool ReadBufferFromFileDecorator::nextImpl()
{
    swap(*impl);
    auto result = impl->next();
    swap(*impl);
    return result;
}

}
