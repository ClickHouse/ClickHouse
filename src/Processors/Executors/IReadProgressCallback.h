#pragma once
#include <memory>

namespace DB
{

/// An interface for read progress callback.
class IReadProgressCallback
{
public:
    virtual ~IReadProgressCallback() = default;
    virtual bool onProgress(uint64_t read_rows, uint64_t read_bytes) = 0;
};

using ReadProgressCallbackPtr = std::unique_ptr<IReadProgressCallback>;


}
