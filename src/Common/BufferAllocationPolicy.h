#pragma once

#include "config.h"

#include <memory>

namespace DB
{

class BufferAllocationPolicy;
using BufferAllocationPolicyPtr = std::unique_ptr<BufferAllocationPolicy>;

///  Buffer number starts with 0
class BufferAllocationPolicy
{
public:

    struct Settings
    {
        size_t strict_size = 0;
        size_t min_size = 16 * 1024 * 1024;
        size_t max_size = 5ULL * 1024 * 1024 * 1024;
        size_t multiply_factor = 2;
        size_t multiply_parts_count_threshold = 500;
        size_t max_single_size = 32 * 1024 * 1024; /// Max size for a single buffer/block
    };

    virtual size_t getBufferNumber() const = 0;
    virtual size_t getBufferSize() const = 0;
    virtual void nextBuffer() = 0;
    virtual ~BufferAllocationPolicy() = 0;

    static BufferAllocationPolicyPtr create(Settings settings_);

};

}
