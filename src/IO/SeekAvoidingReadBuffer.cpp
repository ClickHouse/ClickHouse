#include <IO/SeekAvoidingReadBuffer.h>


namespace DB
{

SeekAvoidingReadBuffer::SeekAvoidingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> impl_, UInt64 min_bytes_for_seek_)
    : ReadBufferFromFileDecorator(std::move(impl_))
    , min_bytes_for_seek(min_bytes_for_seek_)
{
}


off_t SeekAvoidingReadBuffer::seek(off_t off, int whence)
{
    off_t position = getPosition();

    if (whence == SEEK_CUR)
    {
        off += position;
        whence = SEEK_SET;
    }

    if (whence == SEEK_SET && off >= position && off < position + static_cast<off_t>(min_bytes_for_seek))
    {
        swap(*impl);
        impl->ignore(off - position);
        swap(*impl);
        return off;
    }

    return ReadBufferFromFileDecorator::seek(off, whence);
}

}
