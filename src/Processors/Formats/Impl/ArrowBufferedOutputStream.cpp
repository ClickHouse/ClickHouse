#include "ArrowBufferedOutputStream.h"

#if USE_PARQUET || USE_ARROW

#include <arrow/status.h>

namespace DB
{

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & ostr_) : ostr(ostr_) { is_open = true; }

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
