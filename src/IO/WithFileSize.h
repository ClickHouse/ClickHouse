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

/// Return nullopt if couldn't find out file size;
std::optional<size_t> tryGetFileSizeFromReadBuffer(ReadBuffer & in);

size_t getDataOffsetMaybeCompressed(const ReadBuffer & in);

}
