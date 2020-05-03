#include "ArrowBufferedStreams.h"

#if USE_PARQUET || USE_ARROW

#include <arrow/buffer.h>
#include <arrow/status.h>

namespace DB
{

ArrowBufferedInputStream::ArrowBufferedInputStream(ReadBuffer & istr_) : istr{istr_}, is_open{true}
{
}

::arrow::Status ArrowBufferedInputStream::Close()
{
    is_open = false;
    return ::arrow::Status::OK();
}

::arrow::Status ArrowBufferedInputStream::Tell(int64_t * position) const
{
    *position = total_length;
    return ::arrow::Status::OK();
}

::arrow::Status ArrowBufferedInputStream::Read(int64_t nbytes, int64_t * bytes_read, void * out)
{
    *bytes_read = istr.read(reinterpret_cast<char *>(out), nbytes);
    return ::arrow::Status::OK();
}

::arrow::Status ArrowBufferedInputStream::Read(int64_t nbytes, std::shared_ptr<::arrow::Buffer> * out)
{
    std::shared_ptr<::arrow::ResizableBuffer> buffer;
    ARROW_RETURN_NOT_OK(AllocateResizableBuffer(nbytes, &buffer));

    int64_t bytes_read = 0;
    ARROW_RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
    if (bytes_read < nbytes) {
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
    }

    *out = buffer;
    return ::arrow::Status::OK();
}

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & ostr_) : ostr{ostr_}, is_open{true}
{
}

::arrow::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return ::arrow::Status::OK();
}

::arrow::Status ArrowBufferedOutputStream::Tell(int64_t * position) const
{
    *position = total_length;
    return ::arrow::Status::OK();
}

::arrow::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    ostr.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return ::arrow::Status::OK();
}

}

#endif
