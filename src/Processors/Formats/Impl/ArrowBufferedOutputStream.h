#pragma once
#include "config_formats.h"
#if USE_PARQUET || USE_ARROW

#include <IO/WriteBuffer.h>
#include <arrow/io/interfaces.h>

namespace DB
{
class ArrowBufferedOutputStream : public arrow::io::OutputStream
{
public:
    explicit ArrowBufferedOutputStream(WriteBuffer & ostr_);

    /// FileInterface
    ::arrow::Status Close() override;

    ::arrow::Status Tell(int64_t * position) const override;

    bool closed() const override { return !is_open; }

    /// Writable
    ::arrow::Status Write(const void * data, int64_t length) override;

private:
    WriteBuffer & ostr;
    int64_t total_length = 0;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowBufferedOutputStream);
};

}

#endif
