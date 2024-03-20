#pragma once

#include "config.h"

#include "logger_useful.h"

#include <list>

namespace DB
{

///  Buffer number starts with 0
class IBufferAllocationPolicy
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
    virtual ~IBufferAllocationPolicy() = 0;

    using IBufferAllocationPolicyPtr = std::unique_ptr<IBufferAllocationPolicy>;

    static IBufferAllocationPolicyPtr create(Settings settings_);

};

}
