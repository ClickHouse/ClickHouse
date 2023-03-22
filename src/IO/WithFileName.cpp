#include <IO/WithFileName.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/ParallelReadBuffer.h>

namespace DB
{

template <typename T>
static String getFileName(const T & entry)
{
    if (const auto * with_file_name = dynamic_cast<const WithFileName *>(&entry))
        return with_file_name->getFileName();
    return "";
}

String getFileNameFromReadBuffer(const ReadBuffer & in)
{
    if (const auto * compressed = dynamic_cast<const CompressedReadBufferWrapper *>(&in))
        return getFileName(compressed->getWrappedReadBuffer());
    else if (const auto * parallel = dynamic_cast<const ParallelReadBuffer *>(&in))
        return getFileName(parallel->getReadBufferFactory());
    else
        return getFileName(in);
}

}
