#include <IO/SeekAvoidingReadBuffer.h>


namespace DB
{

SeekAvoidingReadBuffer::SeekAvoidingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> nested_, UInt64 min_bytes_for_seek_)
    : nested(std::move(nested_))
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    swap(*nested);
}


std::string SeekAvoidingReadBuffer::getFileName() const
{
    return nested->getFileName();
}


off_t SeekAvoidingReadBuffer::getPosition()
{
    swap(*nested);
    off_t position = nested->getPosition();
    swap(*nested);
    return position;
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
        swap(*nested);
        nested->ignore(off - position);
        swap(*nested);
        position = off;
    }
    else
    {
        swap(*nested);
        position = nested->seek(off, whence);
        swap(*nested);
    }

    return position;
}


bool SeekAvoidingReadBuffer::nextImpl()
{
    swap(*nested);
    bool nested_result = nested->next();
    swap(*nested);
    return nested_result;
}

}
