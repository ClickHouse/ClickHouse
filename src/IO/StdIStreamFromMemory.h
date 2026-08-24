#pragma once

#include <iostream>

namespace DB
{

/// StdIStreamFromMemory is used in WriteBufferFromS3 as a stream which is passed to the S3::Client
/// It provides istream interface (only reading) over the memory.
/// However S3::Client requires iostream interface it only reads from the stream

class StdIStreamFromMemory : public std::iostream
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

    MemoryBuf mem_buf;

public:
    StdIStreamFromMemory(char * begin_, size_t size_);
};

}
