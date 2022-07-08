#pragma once
#include <base/types.h>
#include <optional>

namespace DB
{

class ReadBuffer;

class WithFileSize
{
public:
    virtual size_t getFileSize() = 0;
    virtual ~WithFileSize() = default;
};

bool isBufferWithFileSize(const ReadBuffer & in);

size_t getFileSizeFromReadBuffer(ReadBuffer & in);

}
