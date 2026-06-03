#pragma once
#include <base/types.h>
#include <optional>

namespace DB
{

class ReadBuffer;

class WithFileSize
{
public:
    /// Returns nullopt if couldn't find out file size;
    virtual std::optional<size_t> tryGetFileSize() = 0;
    virtual ~WithFileSize() = default;

    size_t getFileSize();
};

bool isBufferWithFileSize(const ReadBuffer & in);

size_t getFileSizeFromReadBuffer(ReadBuffer & in);
std::optional<size_t> tryGetFileSizeFromReadBuffer(ReadBuffer & in);

size_t getDataOffsetMaybeCompressed(const ReadBuffer & in);

}
