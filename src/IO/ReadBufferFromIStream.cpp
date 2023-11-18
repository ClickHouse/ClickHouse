#include <IO/ReadBufferFromIStream.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
    extern const int LOGICAL_ERROR;
}

bool ReadBufferFromIStream::nextImpl()
{
    size_t bytes_read = 0;

    /// Special case. Here we can avoid buffering inside `BasicBufferedStreamBuf` and read straight to the `internal_buffer`.
    if (auto * response_buf = dynamic_cast<Poco::Net::HTTPBasicStreamBuf *>(istr.rdbuf()); response_buf)
    {
        if (response_buf->in_avail())
            /// It would mean that we already read some data from this `istr`, while we should have use the direct reading scheme.
            /// TODO: replace with chassert
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected buffer state");

        char * read_to = internal_buffer.begin();
        /// It is necessary to read in a loop, since socket usually returns only data available at the moment.
        while (bytes_read < internal_buffer.size())
        {
            try
            {
                const auto bytes_read_last_time = response_buf->readFromDevice(read_to, internal_buffer.size() - bytes_read);
                if (bytes_read_last_time <= 0)
                {
                    /// We just replicate what `std::istream` does
                    istr.setstate(istr.failbit | istr.eofbit);
                    break;
                }

                bytes_read += bytes_read_last_time;
                read_to += bytes_read_last_time;
            }
            catch (...)
            {
                /// We just replicate what `std::istream` does
                istr.setstate(istr.badbit);
                break;
            }
        }
    }
    else
    {
        /// General case when we will read from `istr`.
        istr.read(internal_buffer.begin(), internal_buffer.size());
        bytes_read = istr.gcount();
    }

    if (!bytes_read)
    {
        if (istr.eof())
            return false;

        if (istr.fail())
            throw Exception(ErrorCodes::CANNOT_READ_FROM_ISTREAM, "Cannot read from istream at offset {}", count());

        throw Exception(ErrorCodes::CANNOT_READ_FROM_ISTREAM, "Unexpected state of istream at offset {}", count());
    }
    else
        working_buffer.resize(bytes_read);

    return true;
}

ReadBufferFromIStream::ReadBufferFromIStream(std::istream & istr_, size_t size)
    : BufferWithOwnMemory<ReadBuffer>(size), istr(istr_)
{
    /// - badbit will be set if some exception will be throw from ios implementation
    /// - failbit can be set when for instance read() reads less data, so we
    ///   cannot set it, since we are requesting to read more data, then the
    ///   buffer has now.
    istr.exceptions(std::ios::badbit);
}

}
