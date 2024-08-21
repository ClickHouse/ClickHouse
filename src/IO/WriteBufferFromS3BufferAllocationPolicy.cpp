#include "config.h"

#if USE_AWS_S3

#include <IO/WriteBufferFromS3.h>

#include <memory>

namespace
{

class FixedSizeBufferAllocationPolicy : public DB::WriteBufferFromS3::IBufferAllocationPolicy
{
    const size_t buffer_size = 0;
    size_t buffer_number = 0;

public:
    explicit FixedSizeBufferAllocationPolicy(const DB::S3Settings::RequestSettings::PartUploadSettings & settings_)
        : buffer_size(settings_.strict_upload_part_size)
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


class ExpBufferAllocationPolicy : public DB::WriteBufferFromS3::IBufferAllocationPolicy
{
    const size_t first_size = 0;
    const size_t second_size = 0;

    const size_t multiply_factor = 0;
    const size_t multiply_threshold = 0;
    const size_t max_size = 0;

    size_t current_size = 0;
    size_t buffer_number = 0;

public:
    explicit ExpBufferAllocationPolicy(const DB::S3Settings::RequestSettings::PartUploadSettings & settings_)
        : first_size(std::max(settings_.max_single_part_upload_size, settings_.min_upload_part_size))
        , second_size(settings_.min_upload_part_size)
        , multiply_factor(settings_.upload_part_size_multiply_factor)
        , multiply_threshold(settings_.upload_part_size_multiply_parts_count_threshold)
        , max_size(settings_.max_upload_part_size)
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

}

namespace DB
{

WriteBufferFromS3::IBufferAllocationPolicy::~IBufferAllocationPolicy() = default;

WriteBufferFromS3::IBufferAllocationPolicyPtr WriteBufferFromS3::ChooseBufferPolicy(const S3Settings::RequestSettings::PartUploadSettings & settings_)
{
    if (settings_.strict_upload_part_size > 0)
        return std::make_unique<FixedSizeBufferAllocationPolicy>(settings_);
    else
        return std::make_unique<ExpBufferAllocationPolicy>(settings_);
}

}

#endif
