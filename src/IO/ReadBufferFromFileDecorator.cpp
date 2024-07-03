#include <IO/ReadBufferFromFileDecorator.h>


namespace DB
{

ReadBufferFromFileDecorator::ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_)
    : ReadBufferFromFileDecorator(std::move(impl_), "")
{
}


ReadBufferFromFileDecorator::ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_, const String & file_name_)
    : impl(std::move(impl_)), file_name(file_name_)
{
    swap(*impl);
}


std::string ReadBufferFromFileDecorator::getFileName() const
{
    if (!file_name.empty())
        return file_name;

    return getFileNameFromReadBuffer(*impl);
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

size_t ReadBufferFromFileDecorator::getFileSize()
{
    return getFileSizeFromReadBuffer(*impl);
}

}
