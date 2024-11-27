#include "BufferAllocationPolicy.h"

#include <memory>

namespace DB
{

class FixedSizeBufferAllocationPolicy : public BufferAllocationPolicy
{
    const size_t buffer_size = 0;
    size_t buffer_number = 0;

public:
    explicit FixedSizeBufferAllocationPolicy(const BufferAllocationPolicy::Settings & settings_)
        : buffer_size(settings_.strict_size)
    {
        chassert(buffer_size > 0);
    }

    size_t getBufferNumber() const override { return buffer_number; }

    size_t getBufferSize() const override
    {
        chassert(buffer_number > 0);
        return buffer_size;
    }

    void nextBuffer() override
    {
        ++buffer_number;
    }
};


class ExpBufferAllocationPolicy : public DB::BufferAllocationPolicy
{
    const size_t first_size = 0;
    const size_t second_size = 0;

    const size_t multiply_factor = 0;
    const size_t multiply_threshold = 0;
    const size_t max_size = 0;

    size_t current_size = 0;
    size_t buffer_number = 0;

public:
    explicit ExpBufferAllocationPolicy(const BufferAllocationPolicy::Settings & settings_)
        : first_size(std::max(settings_.max_single_size, settings_.min_size))
        , second_size(settings_.min_size)
        , multiply_factor(settings_.multiply_factor)
        , multiply_threshold(settings_.multiply_parts_count_threshold)
        , max_size(settings_.max_size)
    {
        chassert(first_size > 0);
        chassert(second_size > 0);
        chassert(multiply_factor >= 1);
        chassert(multiply_threshold > 0);
        chassert(max_size > 0);
    }

    size_t getBufferNumber() const override { return buffer_number; }

    size_t getBufferSize() const override
    {
        chassert(buffer_number > 0);
        return current_size;
    }

    void nextBuffer() override
    {
        ++buffer_number;

        if (1 == buffer_number)
        {
            current_size = first_size;
            return;
        }

        if (2 == buffer_number)
            current_size = second_size;

        if (0 == ((buffer_number - 1) % multiply_threshold))
        {
            current_size *= multiply_factor;
            current_size = std::min(current_size, max_size);
        }
    }
};


BufferAllocationPolicy::~BufferAllocationPolicy() = default;

BufferAllocationPolicyPtr BufferAllocationPolicy::create(BufferAllocationPolicy::Settings settings_)
{
    if (settings_.strict_size > 0)
        return std::make_unique<FixedSizeBufferAllocationPolicy>(settings_);
    return std::make_unique<ExpBufferAllocationPolicy>(settings_);
}

}

