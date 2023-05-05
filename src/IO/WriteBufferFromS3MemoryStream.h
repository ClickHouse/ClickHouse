#pragma once

#include "config.h"

#if USE_AWS_S3

#include <iostream>

namespace DB
{

struct MemoryStream: std::iostream
{
    struct MemoryBuf: std::streambuf
    {
            MemoryBuf(char * begin_, size_t size_);

            int_type underflow() override;

            pos_type seekoff(off_type off, std::ios_base::seekdir way,
                             std::ios_base::openmode mode) override;

            pos_type seekpos(pos_type sp,
                             std::ios_base::openmode mode) override;

            char * begin = nullptr;
            size_t size = 0;
    };

    MemoryStream(char * begin_, size_t size_);

    MemoryBuf mem_buf;
};

}

#endif
