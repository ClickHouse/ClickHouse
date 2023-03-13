#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Disks/IDisk.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/StorageS3Settings.h>


namespace DB
{

class S3ParamsForNativeCopyToDisk : public IParamsForNativeCopyToDisk
{
public:
    String src_bucket;
    String src_key;
    size_t src_size = static_cast<size_t>(-1);
    std::optional<ThreadPoolCallbackRunner<void>> scheduler;
    std::optional<S3Settings::RequestSettings> request_settings;
};

}

#endif
