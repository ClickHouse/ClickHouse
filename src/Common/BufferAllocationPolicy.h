#pragma once

#include "config.h"

#include "logger_useful.h"

#include <list>

namespace DB
{

struct BufferAllocationSettings
{
    size_t strict_upload_part_size = 0;
    size_t min_upload_part_size = 16 * 1024 * 1024;
    size_t max_upload_part_size = 5ULL * 1024 * 1024 * 1024;
    size_t upload_part_size_multiply_factor = 2;
    size_t upload_part_size_multiply_parts_count_threshold = 500;
    size_t max_single_part_upload_size = 32 * 1024 * 1024;
};

class IBufferAllocationPolicy
{
    public:
        virtual size_t getBufferNumber() const = 0;
        virtual size_t getBufferSize() const = 0;
        virtual void nextBuffer() = 0;
        virtual ~IBufferAllocationPolicy() = 0;
};

using IBufferAllocationPolicyPtr = std::unique_ptr<IBufferAllocationPolicy>;

IBufferAllocationPolicyPtr ChooseBufferPolicy(BufferAllocationSettings settings_);

}
