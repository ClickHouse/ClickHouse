#include <IO/ReadBufferFromIStream.h>
#include <Common/Exception.h>

#include <istream>


namespace DB
{

bool ReadBufferFromIStream::nextImpl()
{
    if (eof)
        return false;

    chassert(internal_buffer.begin() != nullptr);
    chassert(!internal_buffer.empty());

    size_t bytes_read = 0;
    char * read_to = internal_buffer.begin();

    /// It is necessary to read in a loop, since socket usually returns only data available at the moment.
    while (bytes_read < internal_buffer.size())
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

    if (bytes_read)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
    }

    return bytes_read;
}

ReadBufferFromIStream::ReadBufferFromIStream(std::istream & istr_, size_t size)
    : BufferWithOwnMemory<ReadBuffer>(size)
    , istr(istr_)
    , stream_buf(dynamic_cast<Poco::Net::HTTPBasicStreamBuf &>(*istr.rdbuf()))
{
}

}
