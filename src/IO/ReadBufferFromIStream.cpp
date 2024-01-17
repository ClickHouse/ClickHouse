#include <IO/ReadBufferFromIStream.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
}

bool ReadBufferFromIStream::nextImpl()
{
    if (eof)
        return false;

    size_t bytes_read = 0;
    char * read_to = internal_buffer.begin();

    /// It is necessary to read in a loop, since socket usually returns only data available at the moment.
    while (bytes_read < internal_buffer.size())
    {
        try
        {
            const auto bytes_read_last_time = stream_buf.readFromDevice(read_to, internal_buffer.size() - bytes_read);
            if (bytes_read_last_time <= 0)
            {
                eof = true;
                break;
            }

            bytes_read += bytes_read_last_time;
            read_to += bytes_read_last_time;
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::CANNOT_READ_FROM_ISTREAM,
                "Cannot read from istream at offset {}: {}",
                count(),
                getCurrentExceptionMessage(/*with_stacktrace=*/true));
        }
    }

    if (bytes_read)
        working_buffer.resize(bytes_read);

    return bytes_read;
}

ReadBufferFromIStream::ReadBufferFromIStream(std::istream & istr_, size_t size)
    : BufferWithOwnMemory<ReadBuffer>(size), istr(istr_), stream_buf(dynamic_cast<Poco::Net::HTTPBasicStreamBuf &>(*istr.rdbuf()))
{
}

}
