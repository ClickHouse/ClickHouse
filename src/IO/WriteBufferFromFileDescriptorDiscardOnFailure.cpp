#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>

namespace ProfileEvents
{
    extern const Event CannotWriteToWriteBufferDiscard;
}

namespace DB
{

void WriteBufferFromFileDescriptorDiscardOnFailure::nextImpl()
{
    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::CannotWriteToWriteBufferDiscard);
            break;  /// Discard
        }

        if (res > 0)
            bytes_written += res;
    }
}

}
