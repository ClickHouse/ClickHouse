#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageS3Settings.h>

namespace DB
{

struct IBufferAllocationPolicy
{
    virtual size_t getNumber() const = 0;
    virtual size_t getSize() const = 0;
    virtual void next() = 0;
    virtual ~IBufferAllocationPolicy() = 0;
};

using IBufferAllocationPolicyPtr = std::unique_ptr<IBufferAllocationPolicy>;

IBufferAllocationPolicyPtr ChooseBufferPolicy(const S3Settings::RequestSettings::PartUploadSettings & settings_);

}

#endif
