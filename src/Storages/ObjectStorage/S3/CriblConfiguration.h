#pragma once

#include "config.h"


#if USE_AWS_S3
#    include <Storages/ObjectStorage/S3/Configuration.h>

namespace DB
{
class CriblConfiguration : public StorageS3Configuration
{
public:
    CriblConfiguration();
    ~CriblConfiguration() override;

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;
};
}

#endif
