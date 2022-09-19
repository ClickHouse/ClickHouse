#include "WithFileSize.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBufferFromFileDecorator.h>

namespace DB
{

template <typename T>
static std::optional<size_t> getFileSize(T & in)
{
    if (auto * with_file_size = dynamic_cast<WithFileSize *>(&in))
    {
        return with_file_size->getFileSize();
    }

    return std::nullopt;
}

std::optional<size_t> getFileSizeFromReadBuffer(ReadBuffer & in)
{
    if (auto * delegate = dynamic_cast<ReadBufferFromFileDecorator *>(&in))
    {
        return getFileSize(delegate->getWrappedReadBuffer());
    }
    else if (auto * compressed = dynamic_cast<CompressedReadBufferWrapper *>(&in))
    {
        return getFileSize(compressed->getWrappedReadBuffer());
    }
    else if (auto * parallel = dynamic_cast<ParallelReadBuffer *>(&in))
    {
        return getFileSize(parallel->getReadBufferFactory());
    }

    return getFileSize(in);
}

bool isBufferWithFileSize(const ReadBuffer & in)
{
    if (const auto * delegate = dynamic_cast<const ReadBufferFromFileDecorator *>(&in))
    {
        return delegate->isWithFileSize();
    }
    else if (const auto * compressed = dynamic_cast<const CompressedReadBufferWrapper *>(&in))
    {
        return isBufferWithFileSize(compressed->getWrappedReadBuffer());
    }
    else if (const auto * parallel = dynamic_cast<const ParallelReadBuffer *>(&in))
    {
        return dynamic_cast<const WithFileSize *>(&parallel->getReadBufferFactory()) != nullptr;
    }

    return dynamic_cast<const WithFileSize *>(&in) != nullptr;
}

}
