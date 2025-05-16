#include "WithFileSize.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/PeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FILE_SIZE;
}

size_t WithFileSize::getFileSize()
{
    if (auto maybe_size = tryGetFileSize())
        return *maybe_size;

    throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size");
}

template <typename T>
static std::optional<size_t> tryGetFileSize(T & in)
{
    if (auto * with_file_size = dynamic_cast<WithFileSize *>(&in))
        return with_file_size->tryGetFileSize();

    return std::nullopt;
}

template <typename T>
static size_t getFileSize(T & in)
{
    if (auto maybe_size = tryGetFileSize(in))
        return *maybe_size;

    throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size");
}

std::optional<size_t> tryGetFileSizeFromReadBuffer(ReadBuffer & in)
{
    if (auto * delegate = dynamic_cast<ReadBufferFromFileDecorator *>(&in))
        return tryGetFileSize(delegate->getWrappedReadBuffer());
    if (auto * compressed = dynamic_cast<CompressedReadBufferWrapper *>(&in))
        return tryGetFileSize(compressed->getWrappedReadBuffer());
    return tryGetFileSize(in);
}

size_t getFileSizeFromReadBuffer(ReadBuffer & in)
{
    if (auto maybe_size = tryGetFileSizeFromReadBuffer(in))
        return *maybe_size;

    throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size");
}

bool isBufferWithFileSize(const ReadBuffer & in)
{
    if (const auto * delegate = dynamic_cast<const ReadBufferFromFileDecorator *>(&in))
    {
        return delegate->isWithFileSize();
    }
    if (const auto * compressed = dynamic_cast<const CompressedReadBufferWrapper *>(&in))
    {
        return isBufferWithFileSize(compressed->getWrappedReadBuffer());
    }

    return dynamic_cast<const WithFileSize *>(&in) != nullptr;
}

size_t getDataOffsetMaybeCompressed(const ReadBuffer & in)
{
    if (const auto * delegate = dynamic_cast<const ReadBufferFromFileDecorator *>(&in))
    {
        return getDataOffsetMaybeCompressed(delegate->getWrappedReadBuffer());
    }
    if (const auto * compressed = dynamic_cast<const CompressedReadBufferWrapper *>(&in))
    {
        return getDataOffsetMaybeCompressed(compressed->getWrappedReadBuffer());
    }
    if (const auto * peekable = dynamic_cast<const PeekableReadBuffer *>(&in))
    {
        return getDataOffsetMaybeCompressed(peekable->getSubBuffer());
    }

    return in.count();
}

}
