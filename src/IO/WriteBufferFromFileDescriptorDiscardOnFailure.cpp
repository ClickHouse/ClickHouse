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
            /// Never send this profile event to trace log because it may cause another
            /// write into the same fd and likely will trigger the same error
            /// and will lead to infinite recursion.
            ProfileEvents::incrementNoTrace(ProfileEvents::CannotWriteToWriteBufferDiscard);
            break;  /// Discard
        }

        if (res > 0)
            bytes_written += res;
    }
}

}
